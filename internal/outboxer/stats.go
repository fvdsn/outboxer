package outboxer

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

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
}

type batchStats struct {
	selected               int
	sent                   int
	poison                 int
	dlq                    int
	keptForRetry           int
	senderErrors           int
	fatalAfterCommitErrors int
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

func (s *appStats) addCommittedBatch(batch batchStats) {
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
}

func (s *appStats) addBatchError() {
	if s == nil {
		return
	}
	s.batchErrors.Add(1)
}

func (s *appStats) snapshotAndReset() statsSnapshot {
	if s == nil {
		return statsSnapshot{}
	}
	return statsSnapshot{
		selected:               s.selected.Swap(0),
		sent:                   s.sent.Swap(0),
		poison:                 s.poison.Swap(0),
		dlq:                    s.dlq.Swap(0),
		keptForRetry:           s.keptForRetry.Swap(0),
		batchesProcessed:       s.batchesProcessed.Swap(0),
		batchErrors:            s.batchErrors.Swap(0),
		senderErrors:           s.senderErrors.Swap(0),
		fatalAfterCommitErrors: s.fatalAfterCommitErrors.Swap(0),
	}
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
	snapshot := a.stats.snapshotAndReset()
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
	if remaining, ok := a.estimateRemainingEvents(ctx); ok {
		attrs = append(attrs, "events_remaining_estimate", remaining)
	}
	slog.Info("Statistics", attrs...)
}

func (a *app) estimateRemainingEvents(ctx context.Context) (int64, bool) {
	if a.db == nil || a.cfg.EventTable == "" {
		return 0, false
	}
	queryCtx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	var estimate float64
	if err := a.db.QueryRowContext(queryCtx, "SELECT reltuples FROM pg_catalog.pg_class WHERE oid = to_regclass($1)", ident(a.cfg.EventTable)).Scan(&estimate); err != nil {
		return 0, false
	}
	if estimate < 0 {
		return 0, false
	}
	return int64(estimate), true
}
