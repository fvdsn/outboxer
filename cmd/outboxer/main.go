// Package main is the outboxer command-line entry point.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fvdsn/outboxer/internal/outboxer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := outboxer.Run(ctx, os.Args[1:]); err != nil {
		slog.Error("Fatal error", "error", err.Error())
		os.Exit(1)
	}
}
