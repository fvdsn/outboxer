package main

import (
	"database/sql"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	runModePoll     = "poll"
	runModeOnce     = "once"
	runModeOnDemand = "ondemand"
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
	logInfo(map[string]any{"message": "Shutdown requested by host"})
	_ = db.Close()
	logInfo(map[string]any{"message": "Graceful shutdown"})
	os.Exit(0)
}
