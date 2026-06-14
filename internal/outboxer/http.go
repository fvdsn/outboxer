package outboxer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

func (a *app) serveHTTPRequests() *http.Server {
	server := a.newHTTPServer()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err.Error())
			os.Exit(1)
		}
	}()

	slog.Info("Health server listening", "port", a.cfg.HealthPort)
	return server
}

func (a *app) newHTTPServer() *http.Server {
	server := &http.Server{Addr: fmt.Sprintf(":%d", a.cfg.HealthPort)}

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		method := strings.ToUpper(req.Method)
		switch {
		case method == http.MethodDelete:
			origin := req.Header.Get("x-forwarded-for")
			if origin == "" && req.RemoteAddr != "" {
				origin = req.RemoteAddr
			}
			if origin == "" {
				origin = "unknown"
			}
			slog.Info("Shutdown requested by client", "from", origin)
			_ = a.db.Close()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Shutting down"))
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(ctx)
				slog.Info("Graceful shutdown")
				os.Exit(0)
			}()
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("all good"))
			slog.Debug("Healthcheck request answered")
		}
	})

	return server
}
