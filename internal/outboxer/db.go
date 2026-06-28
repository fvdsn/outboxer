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
	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
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
	if cfg.PubSubEnabled && cfg.SQSEnabled {
		required = append(required, cfg.EventTarget)
	}
	if (cfg.PubSubEnabled && cfg.DefaultPubSubTopic == "") ||
		(cfg.SQSEnabled && cfg.DefaultSQSQueueURL == "") {
		required = append(required, cfg.EventDestination)
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
	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
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

	return fmt.Sprintf(
		`WITH routes AS (`+
			`SELECT resolved_target, resolved_destination, count(*) OVER () AS route_count `+
			`FROM (`+
			`SELECT DISTINCT %s AS resolved_target, %s AS resolved_destination `+
			`FROM %s AS %s WHERE %s`+
			`) AS resolved_routes`+
			`), selected AS (`+
			`SELECT picked.%s AS id, routes.resolved_target, routes.resolved_destination `+
			`FROM routes `+
			`CROSS JOIN LATERAL (`+
			`SELECT %s.%s `+
			`FROM %s AS %s `+
			`WHERE %s AND %s `+
			`ORDER BY %s.%s `+
			`LIMIT GREATEST(1, (($1::bigint + routes.route_count - 1) / routes.route_count))`+
			`) AS picked`+
			`) SELECT selected.resolved_target, selected.resolved_destination, %s.* FROM %s AS %s JOIN selected ON %s.%s = selected.id ORDER BY %s.%s FOR UPDATE`,
		sourceTargetExpr,
		sourceDestinationExpr,
		table,
		ident(sourceAlias),
		sourceRoutablePredicate,
		idCol,
		ident(candidateAlias),
		idCol,
		table,
		ident(candidateAlias),
		candidateRoutablePredicate,
		candidateRouteMatchPredicate,
		ident(candidateAlias),
		idCol,
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
	targetCol := columnSQL(tableAlias, a.cfg.EventTarget)
	switch {
	case a.cfg.EventTarget == "" && a.cfg.PubSubEnabled && !a.cfg.SQSEnabled:
		return sqlStringLiteral(eventTargetPubSub), "TRUE"
	case a.cfg.EventTarget == "" && a.cfg.SQSEnabled && !a.cfg.PubSubEnabled:
		return sqlStringLiteral(eventTargetSQS), "TRUE"
	case a.cfg.PubSubEnabled && !a.cfg.SQSEnabled:
		return fmt.Sprintf("COALESCE(NULLIF(%s, ''), %s)", targetCol, sqlStringLiteral(eventTargetPubSub)),
			fmt.Sprintf("COALESCE(NULLIF(%s, ''), %s) = %s", targetCol, sqlStringLiteral(eventTargetPubSub), sqlStringLiteral(eventTargetPubSub))
	case a.cfg.SQSEnabled && !a.cfg.PubSubEnabled:
		return fmt.Sprintf("COALESCE(NULLIF(%s, ''), %s)", targetCol, sqlStringLiteral(eventTargetSQS)),
			fmt.Sprintf("COALESCE(NULLIF(%s, ''), %s) = %s", targetCol, sqlStringLiteral(eventTargetSQS), sqlStringLiteral(eventTargetSQS))
	default:
		return fmt.Sprintf("NULLIF(%s, '')", targetCol),
			fmt.Sprintf("NULLIF(%s, '') IN (%s, %s)", targetCol, sqlStringLiteral(eventTargetPubSub), sqlStringLiteral(eventTargetSQS))
	}
}

func (a *app) resolvedDestinationSQL(tableAlias string, targetExpr string) string {
	destinationCol := columnSQL(tableAlias, a.cfg.EventDestination)
	switch {
	case a.cfg.EventDestination == "" && a.cfg.PubSubEnabled && !a.cfg.SQSEnabled:
		return sqlStringLiteral(a.cfg.DefaultPubSubTopic)
	case a.cfg.EventDestination == "" && a.cfg.SQSEnabled && !a.cfg.PubSubEnabled:
		return sqlStringLiteral(a.cfg.DefaultSQSQueueURL)
	case a.cfg.EventDestination == "":
		return a.defaultDestinationSQL(targetExpr)
	default:
		return fmt.Sprintf(
			"COALESCE(NULLIF(%s, ''), %s)",
			destinationCol,
			a.defaultDestinationSQL(targetExpr),
		)
	}
}

func (a *app) defaultDestinationSQL(targetExpr string) string {
	return fmt.Sprintf(
		"CASE WHEN %s = %s THEN %s WHEN %s = %s THEN %s ELSE '' END",
		targetExpr,
		sqlStringLiteral(eventTargetPubSub),
		sqlStringLiteral(a.cfg.DefaultPubSubTopic),
		targetExpr,
		sqlStringLiteral(eventTargetSQS),
		sqlStringLiteral(a.cfg.DefaultSQSQueueURL),
	)
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
	switch {
	case a.cfg.PubSubEnabled && !a.cfg.SQSEnabled:
		return sqlStringLiteral(eventTargetPubSub)
	case a.cfg.SQSEnabled && !a.cfg.PubSubEnabled:
		return sqlStringLiteral(eventTargetSQS)
	default:
		return "''"
	}
}

func columnSQL(tableAlias string, name string) string {
	if tableAlias == "" {
		return ident(name)
	}
	return fmt.Sprintf("%s.%s", ident(tableAlias), ident(name))
}

func (a *app) routableSQL(targetExpr string, destinationExpr string, targetPredicate string) string {
	if targetPredicate == "" {
		targetPredicate = fmt.Sprintf("%s IN (%s, %s)", targetExpr, sqlStringLiteral(eventTargetPubSub), sqlStringLiteral(eventTargetSQS))
	}
	return fmt.Sprintf("(%s) AND COALESCE(%s, '') <> '' AND %s", targetPredicate, destinationExpr, a.routeOwnershipSQL(targetExpr, destinationExpr))
}

func (a *app) routeOwnershipSQL(targetExpr string, destinationExpr string) string {
	predicates := []string{}
	if len(a.cfg.PubSubDestinations) > 0 {
		predicates = append(predicates, fmt.Sprintf(
			"(%s <> %s OR %s IN (%s))",
			targetExpr,
			sqlStringLiteral(eventTargetPubSub),
			destinationExpr,
			sqlStringList(a.cfg.PubSubDestinations),
		))
	}
	if len(a.cfg.SQSDestinations) > 0 {
		predicates = append(predicates, fmt.Sprintf(
			"(%s <> %s OR %s IN (%s))",
			targetExpr,
			sqlStringLiteral(eventTargetSQS),
			destinationExpr,
			sqlStringList(a.cfg.SQSDestinations),
		))
	}
	if len(predicates) == 0 {
		return "TRUE"
	}
	return strings.Join(predicates, " AND ")
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
		routeBackend := backendNone
		switch target {
		case eventTargetPubSub:
			routeBackend = backendPubSub
		case eventTargetSQS:
			routeBackend = backendSQS
		default:
			return nil, fmt.Errorf("selected event has unsupported resolved target %q", target)
		}
		destination := valueString(normalizeDBValue(values[1]))
		if destination == "" {
			return nil, fmt.Errorf("selected event has empty resolved destination")
		}

		evt := event{
			columns: map[string]any{},
			route:   eventRoute{backend: routeBackend, destination: destination},
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

func (a *app) deleteEvents(ctx context.Context, tx *sql.Tx, ids []any) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	for i := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		qualifiedIdent(a.cfg.PGSchema, a.cfg.EventTable),
		ident(a.cfg.EventID),
		strings.Join(placeholders, ", "),
	)

	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx, query, ids...)
	return err
}

func ident(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

func qualifiedIdent(schema string, name string) string {
	return pgx.Identifier{schema, name}.Sanitize()
}
