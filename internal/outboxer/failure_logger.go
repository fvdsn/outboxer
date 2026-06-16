package outboxer

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const failureLogWindow = time.Minute

type failureLogger struct {
	mu      sync.Mutex
	window  time.Duration
	now     func() time.Time
	entries map[string]*failureLogEntry
}

type failureLogEntry struct {
	nextLogAt  time.Time
	suppressed int
}

func newFailureLogger(window time.Duration) *failureLogger {
	if window <= 0 {
		window = failureLogWindow
	}
	return &failureLogger{
		window:  window,
		now:     time.Now,
		entries: map[string]*failureLogEntry{},
	}
}

func (a *app) logFailure(ctx context.Context, message string, signature string, attrs ...any) {
	if ctx.Err() != nil {
		return
	}
	if a.failureLogger == nil {
		slog.Error(message, attrs...)
		return
	}
	a.failureLogger.log(message, signature, attrs...)
}

func (l *failureLogger) log(message string, signature string, attrs ...any) {
	if ok, suppressed := l.shouldLog(signature); ok {
		if suppressed > 0 {
			attrs = append(attrs, "suppressed_count", suppressed)
		}
		slog.Error(message, attrs...)
	}
}

func (l *failureLogger) shouldLog(signature string) (bool, int) {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.entries[signature]
	if !ok {
		l.entries[signature] = &failureLogEntry{nextLogAt: now.Add(l.window)}
		return true, 0
	}

	if now.Before(entry.nextLogAt) {
		entry.suppressed++
		return false, 0
	}

	suppressed := entry.suppressed
	entry.suppressed = 0
	entry.nextLogAt = now.Add(l.window)
	return true, suppressed
}
