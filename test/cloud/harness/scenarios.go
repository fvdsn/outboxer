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

	"cloud.google.com/go/pubsub/v2"
	"github.com/jackc/pgx/v5"
)

// Smoke verifies functional behavior against the deployed stack: delivery of
// unordered and ordered events (with real per-key ordering), default and
// explicit destinations, dead-lettering, a drained outbox, and truthful
// /metrics and /healthz.
func Smoke(ctx context.Context, t *testing.T, env Env, db *pgx.Conn, pubsubClient *pubsub.Client) {
	t.Helper()

	const (
		unordered   = 200
		orderedKeys = 3
		perKey      = 50
		poison      = 5
	)
	events := make([]Event, 0, unordered+orderedKeys*perKey+poison)
	for i := 0; i < unordered; i++ {
		destination := env.Topic // explicit destination
		if i%2 == 0 {
			destination = "" // resolved by DEFAULT_PUBSUB_TOPIC
		}
		events = append(events, Event{
			Payload:     fmt.Sprintf("smoke-unordered-%03d", i),
			Destination: destination,
		})
	}
	for k := 0; k < orderedKeys; k++ {
		for i := 0; i < perKey; i++ {
			events = append(events, Event{
				Payload:     fmt.Sprintf("smoke-ordered-key%d-%03d", k, i),
				OrderingKey: fmt.Sprintf("key-%d", k),
			})
		}
	}
	for i := 0; i < poison; i++ {
		events = append(events, Event{
			Payload:     fmt.Sprintf("smoke-poison-%d", i),
			Destination: "syntactically/invalid/topic",
		})
	}
	if err := InsertEvents(ctx, db, events); err != nil {
		t.Fatalf("insert events: %v", err)
	}

	want := unordered + orderedKeys*perKey
	messages := ReceiveMessages(ctx, t, pubsubClient, env.Subscription, want, 3*time.Minute)

	// Every non-poison payload arrives at least once (real Pub/Sub may
	// duplicate), and ordered payloads arrive in order per key.
	seen := map[string]bool{}
	perKeySequences := map[string][]string{}
	for _, msg := range messages {
		seen[msg.Body] = true
		if msg.OrderingKey != "" {
			perKeySequences[msg.OrderingKey] = append(perKeySequences[msg.OrderingKey], msg.Body)
		}
	}
	for _, evt := range events {
		if strings.HasPrefix(evt.Payload, "smoke-poison-") {
			continue
		}
		if !seen[evt.Payload] {
			t.Fatalf("event %s was not delivered", evt.Payload)
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
	if deadLetters != poison {
		t.Fatalf("expected %d dead letters, got %d", poison, deadLetters)
	}

	metrics := NewMetrics(t, env)
	values, err := metrics.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch metrics: %v", err)
	}
	if sent := values["outboxer_events_sent_total"]; sent < float64(want) {
		t.Fatalf("outboxer_events_sent_total = %v, want >= %d", sent, want)
	}
	if dlq := values["outboxer_events_dlq_total"]; dlq != float64(poison) {
		t.Fatalf("outboxer_events_dlq_total = %v, want %d", dlq, poison)
	}
	if backlog := values["outboxer_backlog_events"]; backlog != 0 {
		t.Fatalf("outboxer_backlog_events = %v, want 0 after drain", backlog)
	}
	if code, err := metrics.Healthz(ctx); err != nil || code != http.StatusOK {
		t.Fatalf("/healthz = %d (%v), want 200", code, err)
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
	Elapsed   time.Duration `json:"elapsed_ns"`
	SentTotal float64       `json:"sent_total"`
	Backlog   float64       `json:"backlog_events"`
	Lag       float64       `json:"oldest_event_age_seconds"`
}

// Perf loads n events into the outbox, then samples the relay's own /metrics
// until the backlog drains, reporting sustained throughput and the lag curve.
// The subscription is purged afterwards rather than pulled.
func Perf(ctx context.Context, t *testing.T, environment string, env Env, db *pgx.Conn, pubsubClient *pubsub.Client, n int, resultsDir string) PerfReport {
	t.Helper()

	metrics := NewMetrics(t, env)
	before, err := metrics.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch metrics before run: %v", err)
	}
	baseSent := before["outboxer_events_sent_total"]

	const payloadBytes = 256
	payload := strings.Repeat("x", payloadBytes)
	insertStart := time.Now()
	const chunk = 20000
	for offset := 0; offset < n; offset += chunk {
		size := min(chunk, n-offset)
		events := make([]Event, size)
		for i := range events {
			events[i] = Event{Payload: payload}
		}
		if err := InsertEvents(ctx, db, events); err != nil {
			t.Fatalf("insert events at offset %d: %v", offset, err)
		}
	}
	insertDuration := time.Since(insertStart)
	t.Logf("inserted %d events in %s", n, insertDuration.Round(time.Millisecond))

	report := PerfReport{Environment: environment, Events: n, PayloadBytes: payloadBytes, InsertDuration: insertDuration}
	drainStart := time.Now()
	deadline := drainStart.Add(30 * time.Minute)
	for {
		values, err := metrics.Fetch(ctx)
		if err != nil {
			t.Fatalf("fetch metrics during run: %v", err)
		}
		sample := PerfSample{
			Elapsed:   time.Since(drainStart),
			SentTotal: values["outboxer_events_sent_total"] - baseSent,
			Backlog:   values["outboxer_backlog_events"],
			Lag:       values["outboxer_oldest_event_age_seconds"],
		}
		report.Samples = append(report.Samples, sample)
		report.MaxLagSeconds = max(report.MaxLagSeconds, sample.Lag)
		if sample.SentTotal >= float64(n) && sample.Backlog == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("perf run did not drain within 30m: sent %v of %d, backlog %v", sample.SentTotal, n, sample.Backlog)
		}
		time.Sleep(5 * time.Second)
	}
	report.DrainDuration = time.Since(drainStart)
	report.EventsPerSec = float64(n) / report.DrainDuration.Seconds()

	WaitForEmptyTable(ctx, t, db, "events", time.Minute)
	PurgeSubscription(ctx, t, pubsubClient, env.ProjectID, env.Subscription)

	if resultsDir != "" {
		if err := os.MkdirAll(resultsDir, 0o755); err != nil {
			t.Fatalf("create results dir: %v", err)
		}
		path := filepath.Join(resultsDir, fmt.Sprintf("%s-%s.json", environment, time.Now().UTC().Format("20060102-150405")))
		content, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatalf("marshal report: %v", err)
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatalf("write report: %v", err)
		}
		t.Logf("perf report written to %s", path)
	}

	t.Logf("perf: %d events drained in %s (%.0f events/s, max lag %.1fs)",
		n, report.DrainDuration.Round(time.Second), report.EventsPerSec, report.MaxLagSeconds)
	return report
}
