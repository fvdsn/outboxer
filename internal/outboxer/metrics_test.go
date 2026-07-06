package outboxer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// TestMetricsEndpointServesValidPrometheusExposition renders /metrics and
// feeds it through the official Prometheus exposition parser, so the
// hand-rolled format is verified mechanically rather than by eye.
func TestMetricsEndpointServesValidPrometheusExposition(t *testing.T) {
	stats := newAppStats(time.Now())
	stats.addCommittedBatch(batchStats{
		selected:       12,
		sent:           9,
		poison:         1,
		dlq:            1,
		keptForRetry:   1,
		senderErrors:   2,
		oldestEventAge: 2500 * time.Millisecond,
	}, time.Now())
	stats.addBatchError()
	a := &app{cfg: testConfig(), stats: stats}

	recorder := httptest.NewRecorder()
	a.newHTTPServer().Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain; version=0.0.4") {
		t.Fatalf("unexpected content type %q", contentType)
	}

	// Legacy validation is the strict [a-zA-Z_:][a-zA-Z0-9_:]* name rule our
	// metric names must satisfy.
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(recorder.Body.String()))
	if err != nil {
		t.Fatalf("Prometheus parser rejected /metrics output: %v\n%s", err, recorder.Body.String())
	}

	wantValues := map[string]float64{
		"outboxer_events_selected_total":       12,
		"outboxer_events_sent_total":           9,
		"outboxer_events_poison_total":         1,
		"outboxer_events_dlq_total":            1,
		"outboxer_events_kept_for_retry_total": 1,
		"outboxer_batches_processed_total":     1,
		"outboxer_batch_errors_total":          1,
		"outboxer_sender_errors_total":         2,
		"outboxer_collect_batch_target":        float64(a.cfg.CollectBatchTarget),
		"outboxer_last_batch_selected_events":  12,
		"outboxer_events_pending_retry":        1,
		"outboxer_oldest_event_age_seconds":    2.5,
	}
	for name, want := range wantValues {
		family, ok := families[name]
		if !ok {
			t.Fatalf("metric %s missing from /metrics output", name)
		}
		metric := family.GetMetric()[0]
		got := metric.GetCounter().GetValue() + metric.GetGauge().GetValue()
		if got != want {
			t.Fatalf("metric %s = %v, want %v", name, got, want)
		}
	}
	if _, ok := families["outboxer_last_successful_batch_timestamp_seconds"]; !ok {
		t.Fatal("last successful batch timestamp missing from /metrics output")
	}
}

func TestMetricsOmitLagWithoutTimestampColumn(t *testing.T) {
	cfg := testConfig()
	cfg.EventTimestamp = ""
	a := &app{cfg: cfg, stats: newAppStats(time.Now())}

	recorder := httptest.NewRecorder()
	a.newHTTPServer().Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if strings.Contains(recorder.Body.String(), "outboxer_oldest_event_age_seconds") {
		t.Fatalf("lag metric must be omitted without a timestamp column:\n%s", recorder.Body.String())
	}
}

func TestHealthzReflectsBatchStaleness(t *testing.T) {
	cfg := testConfig()
	cfg.HealthStaleAfter = time.Minute

	statusFor := func(lastSuccess time.Time) int {
		a := &app{cfg: cfg, stats: newAppStats(lastSuccess)}
		recorder := httptest.NewRecorder()
		a.newHTTPServer().Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return recorder.Code
	}

	if code := statusFor(time.Now()); code != http.StatusOK {
		t.Fatalf("fresh relay /healthz = %d, want 200", code)
	}
	if code := statusFor(time.Now().Add(-2 * time.Minute)); code != http.StatusServiceUnavailable {
		t.Fatalf("stale relay /healthz = %d, want 503", code)
	}

	cfg.HealthStaleAfter = 0
	if code := statusFor(time.Now().Add(-24 * time.Hour)); code != http.StatusOK {
		t.Fatalf("disabled staleness check /healthz = %d, want 200", code)
	}
}

func TestHealthzStaysHealthyAcrossSenderErrors(t *testing.T) {
	// A committed batch full of sender errors refreshes health: provider
	// failures are the provider's problem, visible in metrics, not a reason
	// for the scheduler to restart the relay.
	cfg := testConfig()
	cfg.HealthStaleAfter = time.Minute
	a := &app{cfg: cfg, stats: newAppStats(time.Now().Add(-2 * time.Minute))}
	a.stats.addCommittedBatch(batchStats{selected: 5, keptForRetry: 5, senderErrors: 5}, time.Now())

	recorder := httptest.NewRecorder()
	a.newHTTPServer().Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("/healthz after erroring-but-committing batch = %d, want 200", recorder.Code)
	}
}

func TestRootPathStaysUnconditionalLiveness(t *testing.T) {
	cfg := testConfig()
	cfg.HealthStaleAfter = time.Minute
	a := &app{cfg: cfg, stats: newAppStats(time.Now().Add(-24 * time.Hour))}

	recorder := httptest.NewRecorder()
	a.newHTTPServer().Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want unconditional 200", recorder.Code)
	}
}
