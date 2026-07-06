package outboxer

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// handleMetrics serves the Prometheus text exposition format. Every value is
// read from in-memory atomics that the batch loop maintains as a side effect
// of processing, so a scrape touches neither the database nor the processor.
func (a *app) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	a.writeMetrics(w)
}

func (a *app) writeMetrics(w io.Writer) {
	stats := a.stats
	snapshot := stats.snapshot()

	writeCounter(w, "outboxer_events_selected_total",
		"Events selected from the outbox table by committed batches.",
		snapshot.selected)
	writeCounter(w, "outboxer_events_sent_total",
		"Events confirmed by a publishing provider and deleted.",
		snapshot.sent)
	writeCounter(w, "outboxer_events_poison_total",
		"Poison events deleted without a dead-letter table.",
		snapshot.poison)
	writeCounter(w, "outboxer_events_dlq_total",
		"Poison events written to the dead-letter table.",
		snapshot.dlq)
	writeCounter(w, "outboxer_events_kept_for_retry_total",
		"Events left pending after their batch, summed per batch: one event retried across N batches counts N times.",
		snapshot.keptForRetry)
	writeCounter(w, "outboxer_batches_processed_total",
		"Batches committed, including empty ones.",
		snapshot.batchesProcessed)
	writeCounter(w, "outboxer_batch_errors_total",
		"Batches that failed on a database error and rolled back.",
		snapshot.batchErrors)
	writeCounter(w, "outboxer_sender_errors_total",
		"Errors returned by publishing providers; the affected events stay pending.",
		snapshot.senderErrors)
	writeCounter(w, "outboxer_fatal_after_commit_errors_total",
		"Fatal sender errors that stop the relay after committing completed work.",
		snapshot.fatalAfterCommitErrors)

	writeGauge(w, "outboxer_collect_batch_target",
		"Configured COLLECT_BATCH_TARGET; the last batch is saturated when it selected this many events.",
		strconv.Itoa(a.cfg.CollectBatchTarget))
	writeGauge(w, "outboxer_last_batch_selected_events",
		"Events selected by the most recent committed batch.",
		strconv.FormatInt(stats.lastBatchSelected.Load(), 10))
	writeGauge(w, "outboxer_events_pending_retry",
		"Events that failed to send in the most recent committed batch and remain pending.",
		strconv.FormatInt(stats.lastBatchKeptForRetry.Load(), 10))
	if a.cfg.EventTimestamp != "" {
		writeGauge(w, "outboxer_oldest_event_age_seconds",
			"Age of the oldest event selected by the most recent committed batch; 0 when the outbox was empty. This is the outbox lag.",
			formatSeconds(stats.oldestEventAgeMillis.Load()))
	}
	writeGauge(w, "outboxer_last_successful_batch_timestamp_seconds",
		"Unix time of the last committed batch (startup time until the first one).",
		formatSeconds(stats.lastSuccessUnixMilli.Load()))
}

func writeCounter(w io.Writer, name string, help string, value int64) {
	writeMetric(w, name, help, "counter", strconv.FormatInt(value, 10))
}

func writeGauge(w io.Writer, name string, help string, value string) {
	writeMetric(w, name, help, "gauge", value)
}

func writeMetric(w io.Writer, name string, help string, metricType string, value string) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", name, help, name, metricType, name, value)
}

func formatSeconds(millis int64) string {
	return strconv.FormatFloat(float64(millis)/1000, 'f', 3, 64)
}
