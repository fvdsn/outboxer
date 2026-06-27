package outboxer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func notifyIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("OUTBOXER_INTEGRATION_PG_DSN")
	if dsn == "" {
		t.Skip("set OUTBOXER_INTEGRATION_PG_DSN to run the Postgres integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPostgresIntegrationNotifyWakesListener(t *testing.T) {
	db := notifyIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.NotifyChannel = "outboxer_test_" + strconvNano()
	a := &app{cfg: cfg, db: db}

	listener, err := a.startListener(ctx)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer listener.close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = db.ExecContext(ctx, "NOTIFY "+ident(cfg.NotifyChannel))
	}()

	start := time.Now()
	if err := listener.wait(ctx, 5*time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("expected the notification to wake the listener well before the timeout, waited %s", elapsed)
	}
}

func TestPostgresIntegrationNotifyWaitTimesOut(t *testing.T) {
	db := notifyIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.NotifyChannel = "outboxer_test_" + strconvNano()
	a := &app{cfg: cfg, db: db}

	listener, err := a.startListener(ctx)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer listener.close()

	// With no notification, the wait must return cleanly when the timeout
	// elapses (a normal sweep), not error and not block forever.
	start := time.Now()
	if err := listener.wait(ctx, 200*time.Millisecond); err != nil {
		t.Fatalf("expected a timeout to be a clean wake-up, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("expected the wait to block until roughly the timeout, returned after %s", elapsed)
	}
}

// TestPostgresIntegrationNotifyTriggerWakesListener exercises the documented
// operator trigger end to end: an insert fires NOTIFY and wakes the listener.
func TestPostgresIntegrationNotifyTriggerWakesListener(t *testing.T) {
	db := notifyIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suffix := strconvNano()
	table := "outboxer_test_" + suffix
	channel := "outboxer_test_chan_" + suffix
	fn := "outboxer_test_notify_" + suffix

	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (id text PRIMARY KEY, payload text NOT NULL);
		CREATE FUNCTION %s() RETURNS trigger AS $$
		BEGIN
			NOTIFY %s;
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER %s AFTER INSERT ON %s
		FOR EACH STATEMENT EXECUTE FUNCTION %s();
	`, ident(table), ident(fn), ident(channel), ident(fn), ident(table), ident(fn)))
	if err != nil {
		t.Fatalf("create table and trigger: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", ident(fn)))
	}()

	cfg := testConfig()
	cfg.NotifyChannel = channel
	a := &app{cfg: cfg, db: db}

	listener, err := a.startListener(ctx)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer listener.close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, payload) VALUES ('e1', 'hi')", ident(table)))
	}()

	start := time.Now()
	if err := listener.wait(ctx, 5*time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("expected the insert trigger to wake the listener well before the timeout, waited %s", elapsed)
	}
}
