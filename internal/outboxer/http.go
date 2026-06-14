package outboxer

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
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
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Shutting down"))
			a.shutdown()
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("all good"))
			slog.Debug("Healthcheck request answered")
		}
	})

	return server
}
