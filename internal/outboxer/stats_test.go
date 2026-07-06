package outboxer

import (
	"context"
	"testing"
	"time"
)

func TestStatsIntervalSnapshotReportsDeltas(t *testing.T) {
	stats := newAppStats(time.Now())
	stats.addCommittedBatch(batchStats{
		selected:               10,
		sent:                   8,
		poison:                 1,
		dlq:                    0,
		keptForRetry:           1,
		senderErrors:           2,
		fatalAfterCommitErrors: 1,
	}, time.Now())
	stats.addBatchError()

	snapshot := stats.intervalSnapshot()
	if snapshot.selected != 10 || snapshot.sent != 8 || snapshot.poison != 1 || snapshot.dlq != 0 || snapshot.keptForRetry != 1 || snapshot.batchesProcessed != 1 || snapshot.batchErrors != 1 || snapshot.senderErrors != 2 || snapshot.fatalAfterCommitErrors != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if next := stats.intervalSnapshot(); next != (statsSnapshot{}) {
		t.Fatalf("expected an empty interval after the baseline advanced, got %#v", next)
	}

	stats.addCommittedBatch(batchStats{selected: 3, sent: 3}, time.Now())
	if next := stats.intervalSnapshot(); next.selected != 3 || next.sent != 3 || next.batchesProcessed != 1 {
		t.Fatalf("expected only the second batch in the next interval, got %#v", next)
	}

	// Cumulative counters are untouched by interval consumption.
	if total := stats.snapshot(); total.selected != 13 || total.batchesProcessed != 2 {
		t.Fatalf("unexpected cumulative totals: %#v", total)
	}
}

func TestStatsGaugesTrackLastCommittedBatch(t *testing.T) {
	stats := newAppStats(time.Now().Add(-time.Hour))
	committedAt := time.Now()
	stats.addCommittedBatch(batchStats{selected: 7, keptForRetry: 2, oldestEventAge: 1500 * time.Millisecond}, committedAt)

	if got := stats.lastBatchSelected.Load(); got != 7 {
		t.Fatalf("last batch selected = %d, want 7", got)
	}
	if got := stats.lastBatchKeptForRetry.Load(); got != 2 {
		t.Fatalf("last batch kept for retry = %d, want 2", got)
	}
	if got := stats.oldestEventAgeMillis.Load(); got != 1500 {
		t.Fatalf("oldest event age = %dms, want 1500", got)
	}
	if got := stats.lastSuccess(); !got.Equal(committedAt.Truncate(time.Millisecond)) {
		t.Fatalf("last success = %s, want %s", got, committedAt)
	}
}

func TestStartStatsLoggerDisabled(_ *testing.T) {
	a := &app{cfg: testConfig(), stats: &appStats{}}
	a.cfg.StatsInterval = 0
	a.startStatsLogger(context.Background())
	// No assertion needed: this test ensures disabled stats do not panic or block.
}

func TestStatsIntervalConfigDefault(t *testing.T) {
	cfg := testConfig()
	if cfg.StatsInterval != 10*time.Second {
		t.Fatalf("expected test config stats interval, got %s", cfg.StatsInterval)
	}
}
