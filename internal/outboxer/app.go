package outboxer

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

type app struct {
	cfg    appConfig
	db     *sql.DB
	pubsub pubsubPublisher
	sqs    sqsPublisher

	// shutdown cancels the root context, triggering a graceful shutdown of the
	// processing loop. It is called from the HTTP handler and on HTTP server
	// failure.
	shutdown context.CancelFunc

	txMu sync.Mutex
}

// withTimeout derives a context with the given timeout. A non-positive timeout
// disables the deadline and returns the parent context unchanged.
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
