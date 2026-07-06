package outboxer

import (
	"testing"
	"time"
)

func TestWatchdogReportsStallWhenProgressStops(t *testing.T) {
	w := &watchdog{}
	stalled := make(chan struct{})
	stop := w.start(5*time.Millisecond, func() { close(stalled) })
	defer stop()

	select {
	case <-stalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the watchdog to report a stall")
	}
}

func TestWatchdogStaysQuietWhileProgressing(t *testing.T) {
	w := &watchdog{}
	stalled := make(chan struct{})

	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		// Mark progress far more often than the watchdog interval, covering
		// several ticks.
		for i := 0; i < 100; i++ {
			w.markProgress()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	stop := w.start(50*time.Millisecond, func() { close(stalled) })
	<-progressDone
	stop()

	select {
	case <-stalled:
		t.Fatal("watchdog reported a stall while progress was being made")
	default:
	}
}

func TestAppMarkProgressToleratesMissingWatchdog(_ *testing.T) {
	// Tests construct apps without a watchdog; liveness marks must be a no-op.
	a := &app{}
	a.markProgress()
}
