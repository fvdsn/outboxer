package outboxer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

func openDB(cfg appConfig) (*sql.DB, error) {
	pgConfig, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	pgConfig.Host = cfg.PGHost
	pgConfig.Port = cfg.PGPort
	pgConfig.User = cfg.PGUser
	pgConfig.Password = cfg.PGPassword
	pgConfig.Database = cfg.PGDatabase
	pgConfig.ConnectTimeout = cfg.PGConnectTimeout

	if cfg.PGSSL {
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		pgConfig.TLSConfig = tlsConfig
	} else {
		pgConfig.TLSConfig = nil
	}

	db := stdlib.OpenDB(*pgConfig)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	return db, nil
}

func buildTLSConfig(cfg appConfig) (*tls.Config, error) {
	// ServerName is required for certificate hostname verification. pgx only
	// sets it when it builds the TLS config from a connection string, which we
	// bypass by configuring TLS manually.
	tlsConfig := &tls.Config{
		ServerName:         cfg.PGHost,
		InsecureSkipVerify: !cfg.PGSSLRejectUnauthorized,
	}

	if cfg.PGSSLRootCert != "" {
		pem, err := os.ReadFile(cfg.PGSSLRootCert)
		if err != nil {
			return nil, fmt.Errorf("read PG SSL root cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in PG SSL root cert %q", cfg.PGSSLRootCert)
		}
		tlsConfig.RootCAs = pool
	}

	return tlsConfig, nil
}

func (a *app) checkDBWorks(ctx context.Context) error {
	ctx, cancel := provider.WithTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", qualifiedIdent(a.cfg.PGSchema, a.cfg.EventTable))
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	return validateEventColumns(a.cfg, columns)
}

// validateEventColumns verifies that the event table exposes every column the
// current configuration depends on. Optional columns (timestamp, options, and any
// column covered by a default) may be absent.
func validateEventColumns(cfg appConfig, columns []string) error {
	present := map[string]bool{}
	for _, column := range columns {
		present[column] = true
	}

	required := []string{cfg.EventID, cfg.EventPayload}
	if cfg.MaxEventAge > 0 {
		required = append(required, cfg.EventTimestamp)
	}
	routes := configuredProviderRoutes(cfg)
	if len(routes) > 1 {
		required = append(required, cfg.EventTarget)
	}
	for _, route := range routes {
		if route.defaultDestination == "" {
			required = append(required, cfg.EventDestination)
			break
		}
	}

	missing := []string{}
	for _, name := range required {
		if name != "" && !present[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("event table %s is missing required columns: %s", cfg.EventTable, strings.Join(missing, ", "))
	}
	return nil
}

func (a *app) selectEvents(ctx context.Context, tx *sql.Tx) ([]event, error) {
	ctx, cancel := provider.WithTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query, args := a.selectEventsQuery()
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanEvents(rows)
}

func (a *app) selectEventsQuery() (string, []any) {
	return a.selectEventsQuerySQL(), []any{a.cfg.CollectBatchTarget}
}

// selectEventsQuerySQL assembles the batch collection query from three named
// pieces so each can be reviewed on its own:
//
//	WITH routes AS (<one row per eligible route, with the route count>),
//	     selected AS (<per route, the ids of its oldest routable events>)
//	SELECT <the full selected rows, locked, in id order>
func (a *app) selectEventsQuerySQL() string {
	table := qualifiedIdent(a.cfg.PGSchema, a.cfg.EventTable)
	idCol := ident(a.cfg.EventID)
	sourceAlias := "route_source"
	candidateAlias := "candidate"
	eventsAlias := "events"

	sourceTargetExpr, sourceTargetPredicate := a.resolvedTargetSQL(sourceAlias)
	sourceDestinationExpr := a.resolvedDestinationSQL(sourceAlias, sourceTargetExpr)
	sourceRoutablePredicate := a.routableSQL(sourceTargetExpr, sourceDestinationExpr, sourceTargetPredicate)

	candidateTargetExpr, candidateTargetPredicate := a.resolvedTargetSQL(candidateAlias)
	candidateDestinationExpr := a.resolvedDestinationSQL(candidateAlias, candidateTargetExpr)
	candidateRoutablePredicate := a.routableSQL(candidateTargetExpr, candidateDestinationExpr, candidateTargetPredicate)
	candidateRouteMatchPredicate := a.routeMatchSQL(candidateAlias, candidateTargetExpr, candidateDestinationExpr)

	// routes: one row per distinct eligible route; route_count spreads the batch
	// target ($1) evenly across the routes.
	routesCTE := fmt.Sprintf(
		`SELECT resolved_target, resolved_destination, count(*) OVER () AS route_count `+
			`FROM (`+
			`SELECT DISTINCT %s AS resolved_target, %s AS resolved_destination `+
			`FROM %s AS %s WHERE %s`+
			`) AS resolved_routes`,
		sourceTargetExpr,
		sourceDestinationExpr,
		table,
		ident(sourceAlias),
		sourceRoutablePredicate,
	)

	// picked: this route's oldest routable event ids, capped at an even share of
	// the batch target (always at least one so no eligible route starves).
	pickedLateral := fmt.Sprintf(
		`SELECT %s.%s `+
			`FROM %s AS %s `+
			`WHERE %s AND %s `+
			`ORDER BY %s.%s `+
			`LIMIT GREATEST(1, (($1::bigint + routes.route_count - 1) / routes.route_count))`,
		ident(candidateAlias),
		idCol,
		table,
		ident(candidateAlias),
		candidateRoutablePredicate,
		candidateRouteMatchPredicate,
		ident(candidateAlias),
		idCol,
	)

	// selected: the picked ids of every route, each joined to its route.
	selectedCTE := fmt.Sprintf(
		`SELECT picked.%s AS id, routes.resolved_target, routes.resolved_destination `+
			`FROM routes `+
			`CROSS JOIN LATERAL (%s) AS picked`,
		idCol,
		pickedLateral,
	)

	// The final select re-reads the full selected rows and locks them, in id
	// order, for the duration of the batch transaction.
	return fmt.Sprintf(
		`WITH routes AS (%s), selected AS (%s) `+
			`SELECT selected.resolved_target, selected.resolved_destination, %s.* `+
			`FROM %s AS %s JOIN selected ON %s.%s = selected.id `+
			`ORDER BY %s.%s FOR UPDATE`,
		routesCTE,
		selectedCTE,
		ident(eventsAlias),
		table,
		ident(eventsAlias),
		ident(eventsAlias),
		idCol,
		ident(eventsAlias),
		idCol,
	)
}

func (a *app) resolvedTargetSQL(tableAlias string) (expr string, predicate string) {
	routes := configuredProviderRoutes(a.cfg)
	if len(routes) == 0 {
		return "NULL", "FALSE"
	}
	if len(routes) == 1 {
		target := sqlStringLiteral(routes[0].target)
		if a.cfg.EventTarget == "" {
			return target, "TRUE"
		}
		targetCol := columnSQL(tableAlias, a.cfg.EventTarget)
		expr := fmt.Sprintf("COALESCE(NULLIF(%s, ''), %s)", targetCol, target)
		return expr, fmt.Sprintf("%s = %s", expr, target)
	}
	if a.cfg.EventTarget == "" {
		return "NULL", "FALSE"
	}

	targetCol := columnSQL(tableAlias, a.cfg.EventTarget)
	expr = fmt.Sprintf("NULLIF(%s, '')", targetCol)
	return expr, fmt.Sprintf("%s IN (%s)", expr, sqlStringList(providerTargets(routes)))
}

func (a *app) resolvedDestinationSQL(tableAlias string, targetExpr string) string {
	routes := configuredProviderRoutes(a.cfg)
	destinationCol := columnSQL(tableAlias, a.cfg.EventDestination)
	if a.cfg.EventDestination == "" {
		if len(routes) == 1 {
			return sqlStringLiteral(routes[0].defaultDestination)
		}
		return a.defaultDestinationSQL(targetExpr)
	}
	return fmt.Sprintf(
		"COALESCE(NULLIF(%s, ''), %s)",
		destinationCol,
		a.defaultDestinationSQL(targetExpr),
	)
}

func (a *app) defaultDestinationSQL(targetExpr string) string {
	branches := []string{}
	for _, route := range configuredProviderRoutes(a.cfg) {
		branches = append(branches, fmt.Sprintf(
			"WHEN %s = %s THEN %s",
			targetExpr,
			sqlStringLiteral(route.target),
			sqlStringLiteral(route.defaultDestination),
		))
	}
	return "CASE " + strings.Join(branches, " ") + " ELSE '' END"
}

func (a *app) routeMatchSQL(tableAlias string, targetExpr string, destinationExpr string) string {
	targetMatch := fmt.Sprintf("%s = routes.resolved_target", targetExpr)
	if a.cfg.EventTarget != "" {
		targetCol := columnSQL(tableAlias, a.cfg.EventTarget)
		if defaultTarget := a.defaultTargetSQL(); defaultTarget != "''" {
			targetMatch = fmt.Sprintf(
				"(NULLIF(%s, '') = routes.resolved_target OR (NULLIF(%s, '') IS NULL AND %s = routes.resolved_target))",
				targetCol,
				targetCol,
				defaultTarget,
			)
		} else {
			targetMatch = fmt.Sprintf("%s = routes.resolved_target", targetCol)
		}
	}

	destinationMatch := fmt.Sprintf("%s = routes.resolved_destination", destinationExpr)
	if a.cfg.EventDestination != "" {
		destinationCol := columnSQL(tableAlias, a.cfg.EventDestination)
		destinationMatch = fmt.Sprintf(
			"(NULLIF(%s, '') = routes.resolved_destination OR (NULLIF(%s, '') IS NULL AND %s = routes.resolved_destination))",
			destinationCol,
			destinationCol,
			a.defaultDestinationSQL(targetExpr),
		)
	}

	return fmt.Sprintf("(%s) AND (%s)", targetMatch, destinationMatch)
}

func (a *app) defaultTargetSQL() string {
	routes := configuredProviderRoutes(a.cfg)
	if len(routes) == 1 {
		return sqlStringLiteral(routes[0].target)
	}
	return "''"
}

func columnSQL(tableAlias string, name string) string {
	if tableAlias == "" {
		return ident(name)
	}
	return fmt.Sprintf("%s.%s", ident(tableAlias), ident(name))
}

func (a *app) routableSQL(targetExpr string, destinationExpr string, targetPredicate string) string {
	if targetPredicate == "" {
		targetPredicate = fmt.Sprintf("%s IN (%s)", targetExpr, sqlStringList(providerTargets(configuredProviderRoutes(a.cfg))))
	}
	return fmt.Sprintf("(%s) AND COALESCE(%s, '') <> '' AND %s", targetPredicate, destinationExpr, a.routeOwnershipSQL(targetExpr, destinationExpr))
}

func (a *app) routeOwnershipSQL(targetExpr string, destinationExpr string) string {
	predicates := []string{}
	for _, route := range configuredProviderRoutes(a.cfg) {
		if len(route.ownedDestinations) == 0 {
			continue
		}
		predicates = append(predicates, fmt.Sprintf(
			"(%s <> %s OR %s IN (%s))",
			targetExpr,
			sqlStringLiteral(route.target),
			destinationExpr,
			sqlStringList(route.ownedDestinations),
		))
	}
	if len(predicates) == 0 {
		return "TRUE"
	}
	return strings.Join(predicates, " AND ")
}

func providerTargets(routes []providerRoute) []string {
	targets := make([]string, len(routes))
	for i, route := range routes {
		targets[i] = route.target
	}
	return targets
}

func scanEvents(rows *sql.Rows) ([]event, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) < 2 || columns[0] != "resolved_target" || columns[1] != "resolved_destination" {
		return nil, fmt.Errorf("selected event query did not return resolved route columns")
	}

	events := []event{}
	for rows.Next() {
		values := make([]any, len(columns))
		valuePointers := make([]any, len(columns))
		for i := range values {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			return nil, err
		}

		target := valueString(normalizeDBValue(values[0]))
		if target == "" {
			return nil, fmt.Errorf("selected event has empty resolved target")
		}
		destination := valueString(normalizeDBValue(values[1]))
		if destination == "" {
			return nil, fmt.Errorf("selected event has empty resolved destination")
		}

		evt := event{
			columns: map[string]any{},
			route:   eventRoute{target: target, destination: destination},
		}
		for i, column := range columns[2:] {
			evt.columns[column] = normalizeDBValue(values[i+2])
		}
		events = append(events, evt)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqlStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, sqlStringLiteral(value))
	}
	return strings.Join(quoted, ", ")
}

func normalizeDBValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		copied := make([]byte, len(typed))
		copy(copied, typed)
		return copied
	default:
		return typed
	}
}

// deleteEvents removes the batch's finished rows in one statement. The ids
// travel as a single array parameter, so the statement's parameter count is
// independent of the batch size (Postgres caps parameters at 65535). Postgres
// reports the array's element type from the id column, and pgx encodes each
// opaque id into it — the ids round-trip from that same column.
func (a *app) deleteEvents(ctx context.Context, tx *sql.Tx, ids []provider.EventID) error {
	if len(ids) == 0 {
		return nil
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ANY($1)",
		qualifiedIdent(a.cfg.PGSchema, a.cfg.EventTable),
		ident(a.cfg.EventID),
	)

	ctx, cancel := provider.WithTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx, query, ids)
	return err
}

func ident(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

func qualifiedIdent(schema string, name string) string {
	return pgx.Identifier{schema, name}.Sanitize()
}
