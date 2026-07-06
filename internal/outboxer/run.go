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
	// A local .env file deliberately populates the process environment, not just
	// Outboxer's own settings: credentials like AWS_ACCESS_KEY_ID or
	// GOOGLE_APPLICATION_CREDENTIALS must also reach the provider SDKs, which
	// read the environment themselves.
	loadDotEnv(".env")

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

	if err := cfg.validate(configValidationRelay); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The watchdog starts before the first database connection so a hung startup
	// is caught too. A stall exits the process rather than attempting recovery:
	// the supervisor (systemd, Kubernetes) owns the restart.
	wd := &watchdog{}
	stopWatchdog := wd.start(cfg.WatchdogInterval, func() {
		slog.Error("Deadlock detected, shutting down")
		os.Exit(1)
	})
	defer stopWatchdog()

	db, err := openDB(cfg)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	senders, closeProviders, err := buildProviderSenders(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeProviders()

	a := &app{
		cfg:           cfg,
		db:            db,
		senders:       senders,
		shutdown:      cancel,
		failureLogger: newFailureLogger(failureLogWindow),
		stats:         &appStats{},
		watchdog:      wd,
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
