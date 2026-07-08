package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// WaitForSettledRelay proves that the instance answering /metrics is the one
// processing events. Right after a deploy two instances can briefly coexist
// (platform rollover): the old one may still win the row locks while the
// metrics endpoint routes to the new one. A canary event is inserted until
// the scraped instance's sent counter moves. Canary deliveries are left in
// the sink; scenarios filter by payload prefix and ignore them.
func WaitForSettledRelay(ctx context.Context, t *testing.T, db *pgx.Conn, metrics *Metrics) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for attempt := 0; ; attempt++ {
		before, err := metrics.Fetch(ctx)
		if err != nil {
			t.Fatalf("fetch metrics while settling: %v", err)
		}
		canary := Event{Payload: fmt.Sprintf("settle-canary-%d-%d", time.Now().UnixNano(), attempt)}
		if err := InsertEvents(ctx, db, []Event{canary}); err != nil {
			t.Fatalf("insert canary: %v", err)
		}
		WaitForEmptyTable(ctx, t, db, "events", time.Minute)

		after, err := metrics.Fetch(ctx)
		if err != nil {
			t.Fatalf("fetch metrics while settling: %v", err)
		}
		if after["outboxer_events_sent_total"] > before["outboxer_events_sent_total"] {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("relay did not settle: the scraped instance never processed a canary (an old instance may still hold the work)")
		}
		t.Logf("settling: canary processed by another instance, retrying")
		time.Sleep(5 * time.Second)
	}
}

// Smoke verifies functional behavior against the deployed stack: delivery of
// unordered and ordered events (with real per-key ordering), dead-lettering,
// a drained outbox, and truthful metrics and health. Event construction is
// backend-specific (see SmokeEvents); the assertions are not. All counter
// assertions are deltas, so Smoke is re-runnable against a live stack.
func Smoke(ctx context.Context, t *testing.T, env Env, db *pgx.Conn, sink MessageSink, metrics *Metrics, build SmokeEvents) {
	t.Helper()

	WaitForSettledRelay(ctx, t, db, metrics)
	baseline, err := metrics.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch baseline metrics: %v", err)
	}
	dlqBaseline, err := CountRows(ctx, db, env.DLQTable)
	if err != nil {
		t.Fatalf("count dead letters baseline: %v", err)
	}

	const (
		unordered   = 200
		orderedKeys = 3
		perKey      = 50
		poison      = 5
	)
	prefix := fmt.Sprintf("smoke-%d-", time.Now().UnixNano())
	events := make([]Event, 0, unordered+orderedKeys*perKey+poison)
	deliverable := map[string]bool{}
	for i := 0; i < unordered; i++ {
		evt := build.Unordered(fmt.Sprintf("%sunordered-%03d", prefix, i), i)
		deliverable[evt.Payload] = true
		events = append(events, evt)
	}
	for k := 0; k < orderedKeys; k++ {
		for i := 0; i < perKey; i++ {
			evt := build.Ordered(fmt.Sprintf("%sordered-key%d-%03d", prefix, k, i), fmt.Sprintf("key-%d", k), i)
			deliverable[evt.Payload] = true
			events = append(events, evt)
		}
	}
	for i := 0; i < poison; i++ {
		events = append(events, build.Poison(fmt.Sprintf("%spoison-%d", prefix, i), i))
	}
	if err := InsertEvents(ctx, db, events); err != nil {
		t.Fatalf("insert events: %v", err)
	}

	want := unordered + orderedKeys*perKey
	messages := sink.Receive(ctx, t, prefix, want, 3*time.Minute)

	// Every non-poison payload arrives at least once (real backends may
	// duplicate), and ordered payloads arrive in order per key.
	seen := map[string]bool{}
	perKeySequences := map[string][]string{}
	for _, msg := range messages {
		seen[msg.Body] = true
		if msg.OrderingKey != "" {
			perKeySequences[msg.OrderingKey] = append(perKeySequences[msg.OrderingKey], msg.Body)
		}
	}
	for payload := range deliverable {
		if !seen[payload] {
			t.Fatalf("event %s was not delivered", payload)
		}
	}
	for key, sequence := range perKeySequences {
		deduped := dedupeAdjacent(sequence)
		if !sort.StringsAreSorted(deduped) {
			t.Fatalf("ordering violated for %s: %v", key, deduped)
		}
	}

	WaitForEmptyTable(ctx, t, db, "events", 2*time.Minute)

	deadLetters, err := CountRows(ctx, db, env.DLQTable)
	if err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if deadLetters-dlqBaseline != poison {
		t.Fatalf("expected %d new dead letters, got %d", poison, deadLetters-dlqBaseline)
	}

	values, err := metrics.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch metrics: %v", err)
	}
	if sent := values["outboxer_events_sent_total"] - baseline["outboxer_events_sent_total"]; sent < float64(want) {
		t.Fatalf("outboxer_events_sent_total delta = %v, want >= %d", sent, want)
	}
	if dlq := values["outboxer_events_dlq_total"] - baseline["outboxer_events_dlq_total"]; dlq != float64(poison) {
		t.Fatalf("outboxer_events_dlq_total delta = %v, want %d", dlq, poison)
	}
	if backlog := values["outboxer_backlog_events"]; backlog != 0 {
		t.Fatalf("outboxer_backlog_events = %v, want 0 after drain", backlog)
	}
	if code, err := metrics.Healthz(ctx); err != nil || code != http.StatusOK {
		t.Fatalf("health endpoint = %d (%v), want 200", code, err)
	}
}

func dedupeAdjacent(values []string) []string {
	out := values[:0]
	for i, value := range values {
		if i == 0 || value != values[i-1] {
			out = append(out, value)
		}
	}
	return out
}

// PerfReport is written as JSON after each performance run.
type PerfReport struct {
	Environment    string        `json:"environment"`
	Events         int           `json:"events"`
	PayloadBytes   int           `json:"payload_bytes"`
	InsertDuration time.Duration `json:"insert_duration_ns"`
	DrainDuration  time.Duration `json:"drain_duration_ns"`
	EventsPerSec   float64       `json:"events_per_sec"`
	MaxLagSeconds  float64       `json:"max_lag_seconds"`
	Samples        []PerfSample  `json:"samples"`
}

// PerfSample is one /metrics observation during a performance run.
type PerfSample struct {
	Elapsed      time.Duration `json:"elapsed_ns"`
	SentTotal    float64       `json:"sent_total"`
	Backlog      float64       `json:"backlog_events"`
	Lag          float64       `json:"oldest_event_age_seconds"`
	BatchErrors  float64       `json:"batch_errors_total"`
	SenderErrors float64       `json:"sender_errors_total"`
	DBSeconds    float64       `json:"last_batch_db_seconds"`
	PubSeconds   float64       `json:"last_batch_publish_seconds"`
}

// Perf loads n events into the outbox, then samples the relay's own /metrics
// until the backlog drains, reporting sustained throughput and the lag curve.
// The sink is purged (best effort) afterwards rather than pulled.
func Perf(ctx context.Context, t *testing.T, environment string, db *pgx.Conn, sink MessageSink, metrics *Metrics, n int, resultsDir string) PerfReport {
	t.Helper()

	WaitForSettledRelay(ctx, t, db, metrics)
	before, err := metrics.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch metrics before run: %v", err)
	}
	baseSent := before["outboxer_events_sent_total"]

	// The whole load runs in one transaction, with the table re-ANALYZEd
	// before the commit. The relay sees nothing until the rows and their
	// fresh statistics land together, which removes the two artifacts that
	// made runs incomparable: the relay draining *during* the load (fast
	// platforms finished most work inside the insert window), and the
	// stale-statistics plan stall (the collection query planning against
	// "empty table" statistics right after a bulk load). The drain clock
	// starts exactly at commit on every platform.
	const payloadBytes = 256
	payload := strings.Repeat("x", payloadBytes)
	insertStart := time.Now()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin bulk-load transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	const chunk = 20000
	for offset := 0; offset < n; offset += chunk {
		size := min(chunk, n-offset)
		events := make([]Event, size)
		for i := range events {
			events[i] = Event{Payload: payload}
		}
		if err := InsertEvents(ctx, tx, events); err != nil {
			t.Fatalf("insert events at offset %d: %v", offset, err)
		}
	}
	if _, err := tx.Exec(ctx, "ANALYZE events"); err != nil {
		t.Fatalf("analyze events before commit: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit bulk load: %v", err)
	}
	insertDuration := time.Since(insertStart)
	t.Logf("loaded and analyzed %d events in %s (invisible to the relay until commit)", n, insertDuration.Round(time.Millisecond))

	report := PerfReport{Environment: environment, Events: n, PayloadBytes: payloadBytes, InsertDuration: insertDuration}
	drainStart := time.Now()
	deadline := drainStart.Add(30 * time.Minute)
	for {
		values, err := metrics.Fetch(ctx)
		if err != nil {
			t.Fatalf("fetch metrics during run: %v", err)
		}
		sample := PerfSample{
			Elapsed:      time.Since(drainStart),
			SentTotal:    values["outboxer_events_sent_total"] - baseSent,
			Backlog:      values["outboxer_backlog_events"],
			Lag:          values["outboxer_oldest_event_age_seconds"],
			BatchErrors:  values["outboxer_batch_errors_total"],
			SenderErrors: values["outboxer_sender_errors_total"],
			DBSeconds:    values["outboxer_last_batch_db_seconds"],
			PubSeconds:   values["outboxer_last_batch_publish_seconds"],
		}
		report.Samples = append(report.Samples, sample)
		report.MaxLagSeconds = max(report.MaxLagSeconds, sample.Lag)
		if sample.SentTotal >= float64(n) && sample.Backlog == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("perf run did not drain within 30m: sent %v of %d, backlog %v", sample.SentTotal, n, sample.Backlog)
		}
		// A tuned relay drains 200k events in ~10s; a coarser sampling
		// interval would quantize the drain duration into uselessness.
		time.Sleep(time.Second)
	}
	report.DrainDuration = time.Since(drainStart)
	report.EventsPerSec = float64(n) / report.DrainDuration.Seconds()

	WaitForEmptyTable(ctx, t, db, "events", time.Minute)
	sink.Purge(ctx, t)

	if resultsDir != "" {
		writeReport(t, resultsDir, fmt.Sprintf("%s-%s.json", environment, time.Now().UTC().Format("20060102-150405")), report)
	}

	t.Logf("perf: %d events drained in %s (%.0f events/s, max lag %.1fs)",
		n, report.DrainDuration.Round(time.Second), report.EventsPerSec, report.MaxLagSeconds)
	return report
}

// LatencyReport summarizes per-event end-to-end latency: the time from the
// event's insert to its delivery at a real consumer.
type LatencyReport struct {
	Environment string        `json:"environment"`
	Events      int           `json:"events"`
	P50         time.Duration `json:"p50_ns"`
	P90         time.Duration `json:"p90_ns"`
	P99         time.Duration `json:"p99_ns"`
	Max         time.Duration `json:"max_ns"`
}

// Latency measures idle-state end-to-end latency: single events inserted into
// a quiet relay (exercising the LISTEN/NOTIFY wake-up path), timestamped at
// insert and measured at the consumer. Inserts are spaced out so every event
// finds the relay idle.
func Latency(ctx context.Context, t *testing.T, environment string, db *pgx.Conn, sink MessageSink, metrics *Metrics, n int, resultsDir string) LatencyReport {
	t.Helper()

	WaitForSettledRelay(ctx, t, db, metrics)

	// The consumer must be receiving before the first insert so consumer
	// startup does not count as event latency.
	deliveries, stopReceiving := sink.Stream(ctx, t)
	defer stopReceiving()

	insertedAt := make(map[string]time.Time, n)
	latencies := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		payload := fmt.Sprintf("latency-%d-%03d", time.Now().UnixNano(), i)
		before := time.Now()
		if err := InsertEvents(ctx, db, []Event{{Payload: payload}}); err != nil {
			t.Fatalf("insert latency event %d: %v", i, err)
		}
		insertedAt[payload] = before

		deadline := time.After(30 * time.Second)
	waitDelivery:
		for {
			select {
			case d := <-deliveries:
				receivedAt := time.Now()
				if inserted, ok := insertedAt[d.Body]; ok {
					latencies = append(latencies, receivedAt.Sub(inserted))
					delete(insertedAt, d.Body)
					if d.Body == payload {
						break waitDelivery
					}
				}
			case <-deadline:
				t.Fatalf("event %d not delivered within 30s (%d measured so far)", i, len(latencies))
			}
		}
		// Space inserts so the relay returns to its idle wait.
		time.Sleep(500 * time.Millisecond)
	}
	stopReceiving()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) time.Duration {
		index := int(p*float64(len(latencies))) - 1
		return latencies[max(0, min(index, len(latencies)-1))]
	}
	report := LatencyReport{
		Environment: environment,
		Events:      len(latencies),
		P50:         percentile(0.50),
		P90:         percentile(0.90),
		P99:         percentile(0.99),
		Max:         latencies[len(latencies)-1],
	}

	if resultsDir != "" {
		writeReport(t, resultsDir, fmt.Sprintf("%s-latency-%s.json", environment, time.Now().UTC().Format("20060102-150405")), report)
	}

	t.Logf("latency over %d idle-state events: p50=%s p90=%s p99=%s max=%s",
		report.Events, report.P50.Round(time.Millisecond), report.P90.Round(time.Millisecond),
		report.P99.Round(time.Millisecond), report.Max.Round(time.Millisecond))
	return report
}

func writeReport(t *testing.T, dir string, name string, report any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create results dir: %v", err)
	}
	content, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write report: %v", err)
	}
	t.Logf("report written to %s", path)
}
