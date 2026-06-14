package outboxer

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func Run(ctx context.Context, args []string) error {
	cfg, err := loadConfig(args, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("invalid configuration: %w", err)
	}

	setupLogging(cfg.LogLevel, cfg.LogFormat)

	if err := cfg.validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	startDeadlockDetector(cfg.WatchdogInterval)

	db, err := openDB(cfg)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	a := &app{
		cfg:      cfg,
		db:       db,
		shutdown: cancel,
	}

	if cfg.PubSubEnabled {
		pubsubClient, err := newPubSubClient(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create Pub/Sub client: %w", err)
		}
		defer pubsubClient.Close()
		a.pubsub = &cloudPubSubPublisher{client: pubsubClient}
	}

	if cfg.SQSEnabled {
		sqsClient, err := newSQSClient(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create SQS client: %w", err)
		}
		a.sqs = &awsSQSPublisher{client: sqsClient}
	}

	slog.Info("Startup", "pid", os.Getpid())

	if err := a.checkDBWorks(ctx); err != nil {
		return fmt.Errorf("database check failed: %w", err)
	}

	if cfg.HealthPort > 0 {
		server, err := a.serveHTTPRequests()
		if err != nil {
			return fmt.Errorf("start health server: %w", err)
		}
		defer shutdownServer(server)
	}

	a.processEvents(ctx)
	slog.Info("Graceful shutdown")
	return nil
}

func shutdownServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
