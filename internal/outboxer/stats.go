package outboxer

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// appStats holds cumulative processing counters plus last-batch gauges. The
// counters only grow, so /metrics can expose them as Prometheus counters; the
// periodic log line reports per-interval deltas via intervalSnapshot.
type appStats struct {
	selected               atomic.Int64
	sent                   atomic.Int64
	poison                 atomic.Int64
	dlq                    atomic.Int64
	keptForRetry           atomic.Int64
	batchesProcessed       atomic.Int64
	batchErrors            atomic.Int64
	senderErrors           atomic.Int64
	fatalAfterCommitErrors atomic.Int64

	// Last committed batch gauges. oldestEventAgeMillis is the age of the
	// oldest event the batch selected (0 when the outbox was empty or the
	// timestamp column is not configured), observed with no extra database
	// work because batches select in id order.
	lastBatchSelected     atomic.Int64
	lastBatchKeptForRetry atomic.Int64
	oldestEventAgeMillis  atomic.Int64
	lastSuccessUnixMilli  atomic.Int64

	// backlogEvents is this relay's pending-event depth after the last
	// committed batch; backlogFloor is 1 when that value is only a lower bound
	// (probe capped, probe disabled while truncated, or no batch yet).
	backlogEvents atomic.Int64
	backlogFloor  atomic.Int64

	// lastLogged is the baseline for the periodic log line's deltas, shared by
	// the ticker goroutine and the final shutdown flush.
	mu         sync.Mutex
	lastLogged statsSnapshot
}

// newAppStats seeds the last-success gauge with the startup time, so /healthz
// grants a fresh relay one full staleness window before demanding a batch. The
// backlog starts as a floor of 0: unknown until the first batch commits.
func newAppStats(now time.Time) *appStats {
	stats := &appStats{}
	stats.lastSuccessUnixMilli.Store(now.UnixMilli())
	stats.backlogFloor.Store(1)
	return stats
}

// setBacklog records the relay's pending-event depth; floor marks the value as
// a lower bound rather than an exact count.
func (s *appStats) setBacklog(events int64, floor bool) {
	if s == nil {
		return
	}
	s.backlogEvents.Store(events)
	if floor {
		s.backlogFloor.Store(1)
	} else {
		s.backlogFloor.Store(0)
	}
}

type batchStats struct {
	selected               int
	sent                   int
	poison                 int
	dlq                    int
	keptForRetry           int
	senderErrors           int
	fatalAfterCommitErrors int
	oldestEventAge         time.Duration
}

type statsSnapshot struct {
	selected               int64
	sent                   int64
	poison                 int64
	dlq                    int64
	keptForRetry           int64
	batchesProcessed       int64
	batchErrors            int64
	senderErrors           int64
	fatalAfterCommitErrors int64
}

func (s statsSnapshot) sub(other statsSnapshot) statsSnapshot {
	return statsSnapshot{
		selected:               s.selected - other.selected,
		sent:                   s.sent - other.sent,
		poison:                 s.poison - other.poison,
		dlq:                    s.dlq - other.dlq,
		keptForRetry:           s.keptForRetry - other.keptForRetry,
		batchesProcessed:       s.batchesProcessed - other.batchesProcessed,
		batchErrors:            s.batchErrors - other.batchErrors,
		senderErrors:           s.senderErrors - other.senderErrors,
		fatalAfterCommitErrors: s.fatalAfterCommitErrors - other.fatalAfterCommitErrors,
	}
}

func (s *appStats) addCommittedBatch(batch batchStats, now time.Time) {
	if s == nil {
		return
	}
	s.selected.Add(int64(batch.selected))
	s.sent.Add(int64(batch.sent))
	s.poison.Add(int64(batch.poison))
	s.dlq.Add(int64(batch.dlq))
	s.keptForRetry.Add(int64(batch.keptForRetry))
	s.senderErrors.Add(int64(batch.senderErrors))
	s.fatalAfterCommitErrors.Add(int64(batch.fatalAfterCommitErrors))
	s.batchesProcessed.Add(1)

	s.lastBatchSelected.Store(int64(batch.selected))
	s.lastBatchKeptForRetry.Store(int64(batch.keptForRetry))
	s.oldestEventAgeMillis.Store(batch.oldestEventAge.Milliseconds())
	s.lastSuccessUnixMilli.Store(now.UnixMilli())
}

func (s *appStats) addBatchError() {
	if s == nil {
		return
	}
	s.batchErrors.Add(1)
}

func (s *appStats) snapshot() statsSnapshot {
	if s == nil {
		return statsSnapshot{}
	}
	return statsSnapshot{
		selected:               s.selected.Load(),
		sent:                   s.sent.Load(),
		poison:                 s.poison.Load(),
		dlq:                    s.dlq.Load(),
		keptForRetry:           s.keptForRetry.Load(),
		batchesProcessed:       s.batchesProcessed.Load(),
		batchErrors:            s.batchErrors.Load(),
		senderErrors:           s.senderErrors.Load(),
		fatalAfterCommitErrors: s.fatalAfterCommitErrors.Load(),
	}
}

// intervalSnapshot returns the change since the previous call and advances the
// baseline, so consecutive calls partition the cumulative counters into
// non-overlapping intervals.
func (s *appStats) intervalSnapshot() statsSnapshot {
	if s == nil {
		return statsSnapshot{}
	}
	current := s.snapshot()

	s.mu.Lock()
	defer s.mu.Unlock()
	delta := current.sub(s.lastLogged)
	s.lastLogged = current
	return delta
}

// lastSuccess returns the time of the last committed batch (or startup for a
// relay that has not committed one yet). The zero time means the stats were
// never seeded.
func (s *appStats) lastSuccess() time.Time {
	if s == nil {
		return time.Time{}
	}
	millis := s.lastSuccessUnixMilli.Load()
	if millis == 0 {
		return time.Time{}
	}
	return time.UnixMilli(millis)
}

func (a *app) startStatsLogger(ctx context.Context) {
	if a.cfg.StatsInterval <= 0 || a.stats == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(a.cfg.StatsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.logStats(ctx)
			}
		}
	}()
}

func (a *app) logStats(ctx context.Context) {
	if ctx.Err() != nil || a.stats == nil {
		return
	}
	snapshot := a.stats.intervalSnapshot()
	attrs := []any{
		"stats_interval_ms", int(a.cfg.StatsInterval / time.Millisecond),
		"events_selected", snapshot.selected,
		"events_sent", snapshot.sent,
		"events_poison", snapshot.poison,
		"events_dlq", snapshot.dlq,
		"events_kept_for_retry", snapshot.keptForRetry,
		"batches_processed", snapshot.batchesProcessed,
		"batch_errors", snapshot.batchErrors,
		"sender_errors", snapshot.senderErrors,
		"fatal_after_commit_errors", snapshot.fatalAfterCommitErrors,
	}
	slog.Info("Statistics", attrs...)
}
