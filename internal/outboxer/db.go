package outboxer

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
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
		pgConfig.TLSConfig = &tls.Config{InsecureSkipVerify: !cfg.PGSSLRejectUnauthorized}
	} else {
		pgConfig.TLSConfig = nil
	}

	db := stdlib.OpenDB(*pgConfig)
	db.SetMaxOpenConns(cfg.PGMaxConnections)
	db.SetMaxIdleConns(0)
	return db, nil
}

func (a *app) checkDBWorks(ctx context.Context) error {
	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", ident(a.cfg.EventTable))
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	return a.checkRequiredColumns(columns)
}

// checkRequiredColumns verifies that the event table exposes every column the
// current configuration depends on. Optional columns (timestamp, ordering key,
// attributes, and any column covered by a default) may be absent.
func (a *app) checkRequiredColumns(columns []string) error {
	present := map[string]bool{}
	for _, column := range columns {
		present[column] = true
	}

	required := []string{a.cfg.EventID, a.cfg.EventPayload}
	if a.cfg.PubSubEnabled && a.cfg.SQSEnabled {
		required = append(required, a.cfg.EventTarget)
	}
	if (a.cfg.PubSubEnabled && a.cfg.DefaultPubSubTopic == "") ||
		(a.cfg.SQSEnabled && a.cfg.DefaultSQSQueueURL == "") {
		required = append(required, a.cfg.EventDestination)
	}

	missing := []string{}
	for _, name := range required {
		if name != "" && !present[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("event table %s is missing required columns: %s", a.cfg.EventTable, strings.Join(missing, ", "))
	}
	return nil
}

func (a *app) selectEvents(ctx context.Context, tx *sql.Tx) ([]event, error) {
	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY %s LIMIT $1 FOR UPDATE",
		ident(a.cfg.EventTable),
		ident(a.cfg.EventID),
	)

	rows, err := tx.QueryContext(ctx, query, a.cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
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

		evt := event{columns: map[string]any{}}
		for i, column := range columns {
			evt.columns[column] = normalizeDBValue(values[i])
		}
		events = append(events, evt)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
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

func (a *app) deleteEvent(ctx context.Context, tx *sql.Tx, id any) error {
	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", ident(a.cfg.EventTable), ident(a.cfg.EventID))
	_, err := tx.ExecContext(ctx, query, id)
	return err
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
		ident(a.cfg.EventTable),
		ident(a.cfg.EventID),
		strings.Join(placeholders, ", "),
	)

	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	a.txMu.Lock()
	defer a.txMu.Unlock()
	_, err := tx.ExecContext(ctx, query, ids...)
	return err
}

func ident(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
