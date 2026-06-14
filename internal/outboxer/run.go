package outboxer

import (
	"context"
	"flag"
	"os"
	"time"
)

func Run(ctx context.Context, args []string) {
	cfg, err := loadConfig(args, os.Stderr)
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		logError(map[string]any{"message": "Invalid configuration", "error": err.Error()})
		os.Exit(2)
	}

	startDeadlockDetector(cfg.DeadlockCheckInterval)

	db, err := openDB(cfg)
	if err != nil {
		logError(map[string]any{"message": "Something is wrong with the database", "error": err.Error()})
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}

	pubsubClient, err := newPubSubClient(ctx, cfg)
	if err != nil {
		logError(map[string]any{"message": "Failed to create pubsub client", "error": err.Error()})
		os.Exit(1)
	}
	defer pubsubClient.Close()

	sqsClient, err := newSQSClient(ctx, cfg)
	if err != nil {
		logError(map[string]any{"message": "Failed to create sqs client", "error": err.Error()})
		os.Exit(1)
	}

	a := &app{
		cfg:    cfg,
		db:     db,
		pubsub: &cloudPubSubPublisher{client: pubsubClient},
		sqs:    &awsSQSPublisher{client: sqsClient},
	}

	logInfo(map[string]any{"message": "Startup", "pid": os.Getpid()})

	if err := a.checkDBWorks(ctx); err != nil {
		logError(map[string]any{"message": "Something is wrong with the database", "error": err.Error()})
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}

	go handleSignals(db)

	if cfg.HealthcheckPort > 0 {
		a.serveHTTPRequests()
	}
	a.processEvents(ctx)
}
