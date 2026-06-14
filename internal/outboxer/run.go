package outboxer

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"
)

func Run(ctx context.Context, args []string) {
	cfg, err := loadConfig(args, os.Stderr)
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		slog.Error("Invalid configuration", "error", err.Error())
		os.Exit(2)
	}

	setupLogging(cfg.LogLevel, cfg.LogFormat)

	if err := cfg.validate(); err != nil {
		slog.Error("Invalid configuration", "error", err.Error())
		os.Exit(2)
	}

	startDeadlockDetector(cfg.WatchdogInterval)

	db, err := openDB(cfg)
	if err != nil {
		slog.Error("Something is wrong with the database", "error", err.Error())
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}

	a := &app{
		cfg: cfg,
		db:  db,
	}

	if cfg.PubSubEnabled {
		pubsubClient, err := newPubSubClient(ctx, cfg)
		if err != nil {
			slog.Error("Failed to create Pub/Sub client", "error", err.Error())
			os.Exit(1)
		}
		defer pubsubClient.Close()
		a.pubsub = &cloudPubSubPublisher{client: pubsubClient}
	}

	if cfg.SQSEnabled {
		sqsClient, err := newSQSClient(ctx, cfg)
		if err != nil {
			slog.Error("Failed to create SQS client", "error", err.Error())
			os.Exit(1)
		}
		a.sqs = &awsSQSPublisher{client: sqsClient}
	}

	slog.Info("Startup", "pid", os.Getpid())

	if err := a.checkDBWorks(ctx); err != nil {
		slog.Error("Something is wrong with the database", "error", err.Error())
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}

	go handleSignals(db)

	if cfg.HealthPort > 0 {
		a.serveHTTPRequests()
	}
	a.processEvents(ctx)
}
