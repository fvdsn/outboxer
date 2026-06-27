package outboxer

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// Run dispatches on the first argument: the init subcommand provisions the
// database schema, anything else (or only flags) runs the relay. An unknown
// non-flag first argument is rejected rather than silently starting the relay.
func Run(ctx context.Context, args []string) error {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "init":
			return runInit(ctx, args[1:])
		default:
			return fmt.Errorf("unknown command %q", args[0])
		}
	}
	return runRelay(ctx, args)
}

// runRelay loads configuration, connects to the database and the enabled queue
// backends, and processes the outbox until the context is cancelled. It returns
// an error for any fatal startup or configuration problem.
func runRelay(ctx context.Context, args []string) error {
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
		cfg:           cfg,
		db:            db,
		shutdown:      cancel,
		failureLogger: newFailureLogger(failureLogWindow),
		stats:         &appStats{},
	}

	if cfg.PubSubEnabled {
		pubsubClient, err := newPubSubClient(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create Pub/Sub client: %w", err)
		}
		defer pubsubClient.Close()
		a.pubsub = newCloudPubSubPublisher(pubsubClient, cfg)
		defer func() {
			if err := a.pubsub.Close(); err != nil {
				slog.Error("Failed to close Pub/Sub publisher", "error", err.Error())
			}
		}()
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
	if err := a.checkDLQWorks(ctx); err != nil {
		return fmt.Errorf("DLQ check failed: %w", err)
	}

	if cfg.HealthPort > 0 {
		server, err := a.serveHTTPRequests()
		if err != nil {
			return fmt.Errorf("start health server: %w", err)
		}
		defer shutdownServer(server)
	}
	a.startStatsLogger(ctx)

	if err := a.processEvents(ctx); err != nil {
		return fmt.Errorf("process events: %w", err)
	}
	slog.Info("Graceful shutdown")
	return nil
}

func shutdownServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
