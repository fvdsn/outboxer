package outboxer

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestEstimateRemainingEventsUsesPgClassEstimate(t *testing.T) {
	cfg := testConfig()
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()

	mock.ExpectQuery("SELECT reltuples FROM pg_catalog.pg_class WHERE oid = to_regclass($1)").
		WithArgs(`"events"`).
		WillReturnRows(sqlmock.NewRows([]string{"reltuples"}).AddRow(1234.9))

	remaining, ok := a.estimateRemainingEvents(context.Background())
	if !ok || remaining != 1234 {
		t.Fatalf("estimateRemainingEvents = %d, %t; want 1234, true", remaining, ok)
	}
}

func TestEstimateRemainingEventsSkipsUnavailableEstimate(t *testing.T) {
	cfg := testConfig()
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()

	mock.ExpectQuery("SELECT reltuples FROM pg_catalog.pg_class WHERE oid = to_regclass($1)").
		WithArgs(`"events"`).
		WillReturnError(context.DeadlineExceeded)

	if remaining, ok := a.estimateRemainingEvents(context.Background()); ok || remaining != 0 {
		t.Fatalf("estimateRemainingEvents = %d, %t; want 0, false", remaining, ok)
	}
}

func TestMaybeRefreshRemainingEstimateCachesAndThrottles(t *testing.T) {
	cfg := testConfig() // StatsInterval is 10s
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()

	mock.ExpectQuery("SELECT reltuples FROM pg_catalog.pg_class WHERE oid = to_regclass($1)").
		WithArgs(`"events"`).
		WillReturnRows(sqlmock.NewRows([]string{"reltuples"}).AddRow(1234.0))

	var last time.Time
	a.maybeRefreshRemainingEstimate(context.Background(), &last)
	if remaining, ok := a.stats.loadRemaining(); !ok || remaining != 1234 {
		t.Fatalf("loadRemaining = %d, %t; want 1234, true", remaining, ok)
	}

	// A second call within the stats interval must not query again: only one
	// query is expected, and the cached value must be preserved.
	a.maybeRefreshRemainingEstimate(context.Background(), &last)
	if remaining, ok := a.stats.loadRemaining(); !ok || remaining != 1234 {
		t.Fatalf("expected cached estimate to be preserved, got %d, %t", remaining, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database interactions: %v", err)
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
