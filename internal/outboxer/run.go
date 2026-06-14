package outboxer

import (
	"context"
	"os"
	"time"
)

func Run(ctx context.Context) {
	loadDotEnv(".env")
	cfg := loadConfig()

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

	switch cfg.RunMode {
	case runModePoll:
		server := a.serveHTTPRequests()
		if err := a.processEvents(ctx, cfg.RunMode); err != nil {
			logError(map[string]any{"message": "crashed and exited", "error": err.Error()})
		} else {
			logError(map[string]any{"message": "crashed and exited"})
		}
		_ = db.Close()
		_ = server.Close()
		os.Exit(1)
	case runModeOnDemand:
		a.serveHTTPRequests()
		select {}
	default:
		_ = a.processEvents(ctx, cfg.RunMode)
		_ = db.Close()
		logInfo(map[string]any{"message": "done"})
	}
}
