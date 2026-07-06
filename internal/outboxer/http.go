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
	// The timeouts bound slow or stalled clients so they cannot pin health-server
	// connections open indefinitely.
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.cfg.HealthPort),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("all good"))
		slog.Debug("Healthcheck request answered")
	})

	return server
}
