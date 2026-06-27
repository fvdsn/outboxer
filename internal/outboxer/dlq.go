package outboxer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type poisonEvent struct {
	evt   event
	error string
}

type dlqColumnMetadata struct {
	name            string
	typeName        string
	notNull         bool
	defaultExpr     sql.NullString
	identity        string
	generated       string
	canInsertColumn bool
}

type dlqTableMetadata struct {
	relkind        string
	canInsertTable bool
	columns        []dlqColumnMetadata
}

const dlqMetadataSQL = `
SELECT
    c.relkind::text,
    has_table_privilege(c.oid, 'INSERT') AS can_insert_table,
    a.attname,
    t.typname,
    a.attnotnull,
    pg_get_expr(d.adbin, d.adrelid) AS default_expr,
    a.attidentity::text,
    a.attgenerated::text,
    has_column_privilege(c.oid, a.attname, 'INSERT') AS can_insert_column
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE c.oid = to_regclass($1)
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum`

func (a *app) checkDLQWorks(ctx context.Context) error {
	if a.cfg.DLQTable == "" {
		return nil
	}

	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	metadata, err := a.loadDLQTableMetadata(ctx)
	if err != nil {
		return err
	}
	return validateDLQTableMetadata(a.cfg.DLQTable, metadata)
}

// rowQuerier is satisfied by both *sql.DB and *sql.Tx, so metadata can be loaded
// either on the live relay connection or inside an uncommitted init transaction.
type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (a *app) loadDLQTableMetadata(ctx context.Context) (dlqTableMetadata, error) {
	return loadDLQTableMetadata(ctx, a.db, a.cfg.DLQTable)
}

func loadDLQTableMetadata(ctx context.Context, q rowQuerier, dlqTable string) (dlqTableMetadata, error) {
	// Pass the sanitized table name to to_regclass so quoted/mixed-case names are
	// resolved the same way as the INSERT statement built with ident().
	rows, err := q.QueryContext(ctx, dlqMetadataSQL, ident(dlqTable))
	if err != nil {
		return dlqTableMetadata{}, err
	}
	defer rows.Close()

	metadata := dlqTableMetadata{}
	for rows.Next() {
		column := dlqColumnMetadata{}
		if err := rows.Scan(
			&metadata.relkind,
			&metadata.canInsertTable,
			&column.name,
			&column.typeName,
			&column.notNull,
			&column.defaultExpr,
			&column.identity,
			&column.generated,
			&column.canInsertColumn,
		); err != nil {
			return dlqTableMetadata{}, err
		}
		metadata.columns = append(metadata.columns, column)
	}
	if err := rows.Err(); err != nil {
		return dlqTableMetadata{}, err
	}
	return metadata, nil
}

func validateDLQTableMetadata(table string, metadata dlqTableMetadata) error {
	if len(metadata.columns) == 0 {
		return fmt.Errorf("DLQ table %s does not exist or has no columns", table)
	}
	if metadata.relkind != "r" && metadata.relkind != "p" {
		return fmt.Errorf("DLQ table %s must be an ordinary or partitioned table", table)
	}
	if !metadata.canInsertTable {
		return fmt.Errorf("missing INSERT privilege on DLQ table %s", table)
	}

	columns := map[string]dlqColumnMetadata{}
	for _, column := range metadata.columns {
		columns[column.name] = column
	}

	id, ok := columns["id"]
	if !ok {
		return fmt.Errorf("DLQ table %s is missing required column: id", table)
	}
	if !canOmitColumnFromInsert(id) {
		return fmt.Errorf("DLQ table %s column id must be nullable, generated, identity, or have a default", table)
	}

	event, ok := columns["event"]
	if !ok {
		return fmt.Errorf("DLQ table %s is missing required column: event", table)
	}
	if event.generated != "" {
		return fmt.Errorf("DLQ table %s column event must accept inserted values", table)
	}
	if event.typeName != "json" && event.typeName != "jsonb" {
		return fmt.Errorf("DLQ table %s column event must be json or jsonb, got %s", table, event.typeName)
	}
	if !event.canInsertColumn {
		return fmt.Errorf("missing INSERT privilege on DLQ table %s column event", table)
	}

	blockingColumns := []string{}
	for _, column := range metadata.columns {
		if column.name == "id" || column.name == "event" {
			continue
		}
		if !canOmitColumnFromInsert(column) {
			blockingColumns = append(blockingColumns, column.name)
		}
	}
	if len(blockingColumns) > 0 {
		return fmt.Errorf("DLQ table %s has required columns without defaults that Outboxer does not insert: %s", table, strings.Join(blockingColumns, ", "))
	}

	return nil
}

func canOmitColumnFromInsert(column dlqColumnMetadata) bool {
	return !column.notNull || column.defaultExpr.Valid || column.identity != "" || column.generated != ""
}

func (a *app) insertDeadLetters(ctx context.Context, tx *sql.Tx, poison []poisonEvent) error {
	if a.cfg.DLQTable == "" || len(poison) == 0 {
		return nil
	}

	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1::jsonb)", ident(a.cfg.DLQTable), ident("event"))
	for _, poisoned := range poison {
		payload, err := json.Marshal(a.deadLetterPayload(poisoned))
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, string(payload)); err != nil {
			return err
		}
		markProcessorProgress()
	}
	return nil
}

func (a *app) deadLetterPayload(poisoned poisonEvent) map[string]any {
	route := a.classifyRoute(poisoned.evt)
	target := ""
	destination := ""
	switch route.backend {
	case backendPubSub:
		target = eventTargetPubSub
		destination = a.destinationForBackend(poisoned.evt, backendPubSub)
	case backendSQS:
		target = eventTargetSQS
		destination = a.destinationForBackend(poisoned.evt, backendSQS)
	}

	payload := map[string]any{
		"source_table":     a.cfg.EventTable,
		"dead_lettered_at": time.Now().UTC().Format(time.RFC3339Nano),
		"target":           target,
		"destination":      destination,
		"original_event":   originalEventJSON(poisoned.evt),
	}
	if poisoned.error != "" {
		payload["error"] = poisoned.error
	}
	return payload
}

func originalEventJSON(evt event) map[string]any {
	out := make(map[string]any, len(evt.columns))
	for key, value := range evt.columns {
		out[key] = eventJSONValue(value)
	}
	return out
}

func eventJSONValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		var decoded any
		if json.Unmarshal(typed, &decoded) == nil {
			return decoded
		}
		return string(typed)
	default:
		return typed
	}
}
