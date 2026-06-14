package outboxer

import (
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type app struct {
	cfg    appConfig
	db     *sql.DB
	pubsub pubsubPublisher
	sqs    sqsPublisher

	txMu sync.Mutex
}

func handleSignals(db *sql.DB) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	slog.Info("Shutdown requested by host")
	_ = db.Close()
	slog.Info("Graceful shutdown")
	os.Exit(0)
}
