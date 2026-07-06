package outboxer

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// TestBacklogCountAgainstPostgres verifies the bounded backlog probe on a real
// database: only this relay's routable rows count, other relays' rows are
// scanned past without inflating the result, and hitting the scan cap reports
// a floor.
func TestBacklogCountAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("OUTBOXER_INTEGRATION_PG_DSN")
	if dsn == "" {
		t.Skip("set OUTBOXER_INTEGRATION_PG_DSN to run the Postgres integration test")
	}
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	db := stdlib.OpenDB(*pgConfig)
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS backlog_probe_check (id text PRIMARY KEY, payload text NOT NULL, target text, destination text, options jsonb, timestamp timestamptz)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE backlog_probe_check`) }()

	rows := []struct{ id, target, destination string }{
		{"00-mine", "sqs", "queue-a"},
		{"01-other-destination", "sqs", "queue-b"},
		{"02-mine", "sqs", "queue-a"},
		{"03-disabled-target", "pubsub", "topic-a"},
		{"04-mine", "sqs", "queue-a"},
	}
	for _, row := range rows {
		if _, err := db.ExecContext(ctx, `INSERT INTO backlog_probe_check (id, payload, target, destination) VALUES ($1, 'x', $2, $3)`, row.id, row.target, row.destination); err != nil {
			t.Fatalf("insert %s: %v", row.id, err)
		}
	}

	cfg := testConfig()
	cfg.EventTable = "backlog_probe_check"
	cfg.PubSubEnabled = false
	cfg.SQSEnabled = true
	cfg.SQSDestinations = []string{"queue-a"}
	cfg.BacklogCountLimit = 100
	a := &app{cfg: cfg, db: db}

	count, capped, err := a.countBacklog(ctx)
	if err != nil {
		t.Fatalf("countBacklog: %v", err)
	}
	if count != 3 || capped {
		t.Fatalf("countBacklog = (%d, %t), want (3, false): other relays' rows must not count", count, capped)
	}

	// A scan cap of 2 covers ids 00 and 01, of which one is routable.
	a.cfg.BacklogCountLimit = 2
	count, capped, err = a.countBacklog(ctx)
	if err != nil {
		t.Fatalf("countBacklog capped: %v", err)
	}
	if count != 1 || !capped {
		t.Fatalf("countBacklog = (%d, %t), want (1, true): a capped scan reports a floor", count, capped)
	}
}
