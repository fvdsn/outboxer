package outboxer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// unlistenTimeout bounds the subscription cleanup when the listener releases
// its connection back to the pool.
const unlistenTimeout = time.Second

// notifyListener holds a dedicated PostgreSQL connection subscribed to the
// derived notification channel for the relay's lifetime. Postgres delivers a
// NOTIFY only to sessions listening at commit time, so the subscription is
// persistent: notifications for events committed while a batch is running
// buffer on the connection and the next idle wait returns immediately,
// instead of the event waiting out the poll backstop. The 1s backstop sweep
// remains the durability net — for reconnect gaps after a listener failure,
// a missed notification only delays an event until the next sweep, never
// loses it.
type notifyListener struct {
	conn *sql.Conn
}

// startListener dedicates a pool connection to the notification subscription
// and issues LISTEN on the derived channel. The caller keeps the listener
// across batches and closes it on shutdown or after a wait error.
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

// close releases the listener connection back to the pool, removing the
// subscription first so pooled reuse does not carry LISTEN state. Called on
// shutdown and after a wait error (the connection is then re-established by
// the next idle cycle). If the cleanup fails the connection is in an unknown
// state and is discarded instead of reused. It is safe to call on a nil
// listener.
func (l *notifyListener) close() {
	if l == nil || l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), unlistenTimeout)
	defer cancel()
	if _, err := l.conn.ExecContext(ctx, "UNLISTEN *"); err != nil {
		// Returning driver.ErrBadConn from Raw marks the connection bad, so the
		// pool destroys it on Close instead of handing it to the next batch.
		_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
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
