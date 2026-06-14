package outboxer

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func (a *app) serveHTTPRequests() *http.Server {
	server := a.newHTTPServer()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logError(map[string]any{"message": "HTTP server failed", "error": err.Error()})
			os.Exit(1)
		}
	}()

	logInfo(map[string]any{"message": fmt.Sprintf("Server listening on http://0.0.0.0:%d", a.cfg.HealthcheckPort)})
	return server
}

func (a *app) newHTTPServer() *http.Server {
	server := &http.Server{Addr: fmt.Sprintf(":%d", a.cfg.HealthcheckPort)}

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
			logInfo(map[string]any{"message": "Shutdown requested by client", "from": origin})
			_ = a.db.Close()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Shutting down"))
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(ctx)
				logInfo(map[string]any{"message": "Graceful shutdown"})
				os.Exit(0)
			}()
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("all good"))
			logDebug(map[string]any{"message": "Healthcheck request answered"})
		}
	})

	return server
}
