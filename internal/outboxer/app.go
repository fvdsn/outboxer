package outboxer

import (
	"context"
	"database/sql"
	"sync"
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
