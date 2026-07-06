package outboxer

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// watchdog detects a stuck relay. The processing loop bumps a counter as it
// makes progress; a background goroutine checks on an interval and reports a
// stall when the counter did not change between two consecutive ticks.
type watchdog struct {
	progress atomic.Int64
}

// markProgress records that the processing loop is alive. It is safe for
// concurrent use.
func (w *watchdog) markProgress() {
	w.progress.Add(1)
}

// start launches the watchdog goroutine. onStall is invoked once when no
// progress happened between two consecutive ticks, after which the watchdog
// stops watching. The returned stop function terminates the goroutine.
func (w *watchdog) start(interval time.Duration, onStall func()) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		previous := w.progress.Load()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				current := w.progress.Load()
				if current == previous {
					onStall()
					return
				}
				previous = current
				slog.Debug("Watchdog heartbeat")
			}
		}
	}()
	return func() { close(done) }
}

// markProgress records processing liveness on the app's watchdog. Apps
// constructed without one (tests) treat it as a no-op.
func (a *app) markProgress() {
	if a.watchdog != nil {
		a.watchdog.markProgress()
	}
}
