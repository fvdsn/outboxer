package outboxer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestInitApplyRoundTripIntegration provisions a fresh schema with init --apply
// and confirms the relay's own startup checks pass against it, that a second
// apply is a clean no-op, and that post-apply validation runs. It exercises the
// schema-object path (no role management) so it does not require CREATEROLE or
// leave cluster-wide roles behind.
func TestInitApplyRoundTripIntegration(t *testing.T) {
	dsn := os.Getenv("OUTBOXER_INTEGRATION_PG_DSN")
	if dsn == "" {
		t.Skip("set OUTBOXER_INTEGRATION_PG_DSN to run the Postgres integration test")
	}

	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}

	suffix := strings.ReplaceAll(strconvNano(), "-", "_")
	schema := "outboxer_init_" + suffix
	table := "events"
	dlq := "dead_letters"

	cfg := appConfig{
		EventTable:       table,
		EventID:          "id",
		EventPayload:     "payload",
		EventTarget:      "target",
		EventDestination: "destination",
		EventTimestamp:   "timestamp",
		EventOptions:     "options",
		DLQTable:         dlq,
		NotifyChannel:    "outboxer_" + suffix,
		PGHost:           pc.Host,
		PGPort:           pc.Port,
		PGUser:           pc.User,
		PGPassword:       pc.Password,
		PGDatabase:       pc.Database,
		PGSchema:         schema,
	}

	ctx := context.Background()

	t.Cleanup(func() {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return
		}
		defer db.Close()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", ident(schema)))
	})

	if err := applyInit(ctx, cfg); err != nil {
		t.Fatalf("apply init: %v", err)
	}
	// A second apply must be a clean no-op.
	if err := applyInit(ctx, cfg); err != nil {
		t.Fatalf("re-apply init: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	relay := &app{cfg: cfg, db: db}
	if err := relay.checkDBWorks(ctx); err != nil {
		t.Fatalf("relay event-table check failed against provisioned schema: %v", err)
	}
	if err := relay.checkDLQWorks(ctx); err != nil {
		t.Fatalf("relay DLQ check failed against provisioned schema: %v", err)
	}
}
