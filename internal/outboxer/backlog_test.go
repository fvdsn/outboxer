package outboxer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestBatchDrained(t *testing.T) {
	routeA := eventRoute{target: "sqs", destination: "queue-a"}
	routeB := eventRoute{target: "pubsub", destination: "topic-b"}
	eventsFor := func(route eventRoute, count int) []event {
		events := make([]event, count)
		for i := range events {
			events[i] = event{route: route}
		}
		return events
	}

	tests := []struct {
		name   string
		events []event
		target int
		want   bool
	}{
		{"empty batch", nil, 10, true},
		{"single route under target", eventsFor(routeA, 9), 10, true},
		{"single route at target", eventsFor(routeA, 10), 10, false},
		{"two routes under their shares", append(eventsFor(routeA, 4), eventsFor(routeB, 4)...), 10, true},
		{"one route truncated at its share despite total under target", append(eventsFor(routeA, 5), eventsFor(routeB, 1)...), 10, false},
		{"share of at least one row", eventsFor(routeA, 1), 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := batchDrained(tt.events, tt.target); got != tt.want {
				t.Fatalf("batchDrained = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestBacklogCountSQLUsesRoutingPredicate(t *testing.T) {
	cfg := testConfig()
	cfg.SQSDestinations = []string{"queue-a"}
	a := &app{cfg: cfg}

	query := a.backlogCountSQL()
	for _, expected := range []string{
		"count(*) FILTER (WHERE",
		`FROM "public"."events" AS "backlog"`,
		`ORDER BY "backlog"."id" LIMIT $1`,
		"IN ('queue-a')",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected backlog count query to contain %q, got:\n%s", expected, query)
		}
	}
	if strings.Contains(query, "payload") {
		t.Fatalf("backlog count query must not touch the payload column:\n%s", query)
	}
}

func TestUpdateBacklogDrainedBatchIsExact(t *testing.T) {
	a := &app{cfg: testConfig(), stats: newAppStats(time.Now())}

	a.updateBacklog(context.Background(), batchResult{
		drained: true,
		stats:   batchStats{keptForRetry: 3},
	})

	if got := a.stats.backlogEvents.Load(); got != 3 {
		t.Fatalf("backlog = %d, want 3", got)
	}
	if got := a.stats.backlogFloor.Load(); got != 0 {
		t.Fatalf("drained backlog must be exact, floor = %d", got)
	}
}

func TestUpdateBacklogTruncatedBatchWithoutProbeIsFloor(t *testing.T) {
	cfg := testConfig()
	cfg.BacklogCountLimit = 0
	a := &app{cfg: cfg, stats: newAppStats(time.Now())}

	a.updateBacklog(context.Background(), batchResult{
		drained: false,
		stats:   batchStats{keptForRetry: 2},
	})

	if got := a.stats.backlogEvents.Load(); got != 2 {
		t.Fatalf("backlog = %d, want 2", got)
	}
	if got := a.stats.backlogFloor.Load(); got != 1 {
		t.Fatalf("truncated backlog without a probe must be a floor, floor = %d", got)
	}
}

func TestUpdateBacklogTruncatedBatchProbesAndThrottles(t *testing.T) {
	cfg := testConfig()
	cfg.BacklogCountLimit = 100
	cfg.StatsInterval = time.Hour
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.stats = newAppStats(time.Now())

	mock.ExpectQuery(a.backlogCountSQL()).
		WithArgs(100).
		WillReturnRows(sqlmock.NewRows([]string{"routable", "scanned"}).AddRow(42, 100))

	truncated := batchResult{drained: false, stats: batchStats{keptForRetry: 5}}
	a.updateBacklog(context.Background(), truncated)

	if got := a.stats.backlogEvents.Load(); got != 42 {
		t.Fatalf("backlog = %d, want 42", got)
	}
	if got := a.stats.backlogFloor.Load(); got != 1 {
		t.Fatalf("a probe that hit its scan cap must report a floor, floor = %d", got)
	}

	// A second truncated batch within the probe interval must not query again;
	// the sqlmock cleanup fails on any unexpected query.
	a.updateBacklog(context.Background(), truncated)
	if got := a.stats.backlogEvents.Load(); got != 42 {
		t.Fatalf("throttled backlog changed to %d", got)
	}
}
