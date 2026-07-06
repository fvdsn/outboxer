// Package outboxer implements a worker for the transactional outbox pattern: it
// reads events from a PostgreSQL table and publishes them to Google Pub/Sub or
// AWS SQS.
package outboxer

import (
	"context"
	"database/sql"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

type app struct {
	cfg     appConfig
	db      *sql.DB
	senders map[string]provider.Sender

	failureLogger *failureLogger
	stats         *appStats
	watchdog      *watchdog

	// lastBacklogProbe throttles the bounded backlog count; it is only touched
	// by the processing goroutine.
	lastBacklogProbe time.Time

	// shutdown cancels the root context, triggering a graceful shutdown of the
	// processing loop. It is called from the HTTP handler and on HTTP server
	// failure.
	shutdown context.CancelFunc
}
