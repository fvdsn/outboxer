package outboxer

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestConnectionReuseAndListenerCleanup verifies the relay's connection
// economy against a real database: sequential batch transactions reuse one
// backend instead of opening a fresh connection each time, and an idle
// listener cycle releases the connection back clean (no lingering LISTEN
// subscription) and reusable.
func TestConnectionReuseAndListenerCleanup(t *testing.T) {
	dsn := os.Getenv("OUTBOXER_INTEGRATION_PG_DSN")
	if dsn == "" {
		t.Skip("set OUTBOXER_INTEGRATION_PG_DSN to run the Postgres integration test")
	}
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	cfg := testConfig()
	cfg.PGHost = pgConfig.Host
	cfg.PGPort = pgConfig.Port
	cfg.PGUser = pgConfig.User
	cfg.PGPassword = pgConfig.Password
	cfg.PGDatabase = pgConfig.Database
	cfg.NotifyChannel = "conn_reuse_check"

	db, err := openDB(cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	backendPID := func() int {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		var pid int
		if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatalf("backend pid: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		return pid
	}

	first := backendPID()
	for i := 0; i < 4; i++ {
		if pid := backendPID(); pid != first {
			t.Fatalf("transaction %d ran on backend %d, want reuse of %d", i+2, pid, first)
		}
	}

	// An idle listener cycle borrows the connection, LISTENs, and releases it.
	a := &app{cfg: cfg, db: db}
	listener, err := a.startListener(ctx)
	if err != nil {
		t.Fatalf("startListener: %v", err)
	}
	listener.close()

	// The released connection is the same backend, and carries no subscription
	// into the next batch.
	var channels int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_listening_channels()").Scan(&channels); err != nil {
		t.Fatalf("pg_listening_channels: %v", err)
	}
	if channels != 0 {
		t.Fatalf("expected no lingering LISTEN subscriptions after listener release, got %d", channels)
	}
	if pid := backendPID(); pid != first {
		t.Fatalf("post-listener transaction ran on backend %d, want reuse of %d", pid, first)
	}
}
