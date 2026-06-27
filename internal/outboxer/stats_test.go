package outboxer

import (
	"context"
	"testing"
	"time"
)

func TestStatsSnapshotAndReset(t *testing.T) {
	stats := &appStats{}
	stats.addCommittedBatch(batchStats{
		selected:               10,
		sent:                   8,
		poison:                 1,
		dlq:                    0,
		keptForRetry:           1,
		senderErrors:           2,
		fatalAfterCommitErrors: 1,
	})
	stats.addBatchError()

	snapshot := stats.snapshotAndReset()
	if snapshot.selected != 10 || snapshot.sent != 8 || snapshot.poison != 1 || snapshot.dlq != 0 || snapshot.keptForRetry != 1 || snapshot.batchesProcessed != 1 || snapshot.batchErrors != 1 || snapshot.senderErrors != 2 || snapshot.fatalAfterCommitErrors != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if reset := stats.snapshotAndReset(); reset != (statsSnapshot{}) {
		t.Fatalf("expected counters to reset, got %#v", reset)
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
