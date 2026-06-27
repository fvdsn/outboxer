package outboxer

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// notifyListener borrows a PostgreSQL connection for one idle cycle: it LISTENs
// on the configured channel and blocks until a notification arrives or the wait
// times out, then the connection is released. It is used only when polling is
// enabled (POLL_INTERVAL_MS > 0) to wake the processing loop quickly when a
// producer inserts an event. The poll interval remains the durability backstop:
// notifications are best-effort, and a missed one only delays an event until the
// next sweep, never loses it.
type notifyListener struct {
	conn *sql.Conn
}

// startListener borrows a connection and issues LISTEN on the configured
// channel. The caller must close the listener to release the connection.
func (a *app) startListener(ctx context.Context) (*notifyListener, error) {
	conn, err := a.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "LISTEN "+ident(a.cfg.NotifyChannel)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &notifyListener{conn: conn}, nil
}

// close releases the listener connection. It is safe to call on a nil listener.
func (l *notifyListener) close() {
	if l == nil || l.conn == nil {
		return
	}
	_ = l.conn.Close()
}

// wait blocks until a notification arrives or the timeout elapses. Both are
// normal wake-ups and return nil. A non-nil error means the connection itself
// failed and the listener should be discarded and re-established.
func (l *notifyListener) wait(ctx context.Context, timeout time.Duration) error {
	return l.conn.Raw(func(driverConn any) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()

		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if _, err := pgxConn.WaitForNotification(waitCtx); err != nil {
			// A timeout of our wait window, or a parent-context cancellation
			// during shutdown, is a normal wake-up rather than a failure.
			if waitCtx.Err() != nil {
				return nil
			}
			return err
		}

		// Coalesce a burst: drain notifications already buffered on the
		// connection so the inserted rows are handled in a single processing
		// cycle instead of one spurious empty cycle per notification.
		drainNotifications(ctx, pgxConn)
		return nil
	})
}

// drainNotifications consumes notifications already buffered on the connection
// without blocking for new ones.
func drainNotifications(ctx context.Context, conn *pgx.Conn) {
	for {
		drainCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		_, err := conn.WaitForNotification(drainCtx)
		cancel()
		if err != nil {
			return
		}
	}
}
