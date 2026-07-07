package outboxer

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

func (a *app) serveHTTPRequests() (*http.Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", a.cfg.HealthPort))
	if err != nil {
		return nil, err
	}

	server := a.newHTTPServer()

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err.Error())
			a.shutdown()
		}
	}()

	slog.Info("Health server listening", "port", a.cfg.HealthPort)
	return server, nil
}

func (a *app) newHTTPServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", a.handleMetrics)
	mux.HandleFunc("/healthz", a.handleHealthz)
	// Alias: Cloud Run's frontend intercepts /healthz on run.app URLs (Google
	// edge returns 404 before the container is reached), so the staleness
	// check is also served on /health.
	mux.HandleFunc("/health", a.handleHealthz)
	// Every other path answers 200 as a pure liveness signal, matching the
	// original single-endpoint behavior.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("all good"))
		slog.Debug("Healthcheck request answered")
	})

	// The timeouts bound slow or stalled clients so they cannot pin health-server
	// connections open indefinitely.
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", a.cfg.HealthPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// handleHealthz reports 503 when no batch has committed within the configured
// staleness window. Batches with sender errors still commit, so provider
// failures never flip health — only the relay's own loop breaking does. A
// fresh relay's window starts at startup (see newAppStats).
func (a *app) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	threshold := a.cfg.HealthStaleAfter
	if threshold <= 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok (staleness check disabled)"))
		return
	}

	last := a.stats.lastSuccess()
	age := time.Since(last)
	if last.IsZero() || age > threshold {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "unhealthy: no committed batch for %s (threshold %s)", age.Round(time.Second), threshold)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "ok: last committed batch %s ago", age.Round(time.Second))
}
