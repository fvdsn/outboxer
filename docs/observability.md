# Observability

When the HTTP server is enabled (`HEALTH_PORT` > 0) Outboxer serves Prometheus
metrics on `/metrics` and a batch-staleness health check on `/healthz`. Both
are computed from in-memory counters that the batch loop maintains as a side
effect of processing: **a scrape or health probe never queries the database**,
never runs a `COUNT(*)`, and never interrupts the idle `LISTEN` connection.

## Metrics

Counters are cumulative since process start.

| Metric | Type | Meaning |
| --- | --- | --- |
| `outboxer_events_selected_total` | counter | Events selected from the outbox table by committed batches. |
| `outboxer_events_sent_total` | counter | Events confirmed by a provider and deleted. |
| `outboxer_events_poison_total` | counter | Poison events deleted without a DLQ table. |
| `outboxer_events_dlq_total` | counter | Poison events written to the DLQ table. |
| `outboxer_events_kept_for_retry_total` | counter | Events left pending after their batch, summed per batch: one event retried across N batches counts N times. |
| `outboxer_batches_processed_total` | counter | Batches committed, including empty ones. |
| `outboxer_batch_errors_total` | counter | Batches that failed on a database error and rolled back. |
| `outboxer_sender_errors_total` | counter | Errors returned by providers; the affected events stay pending. |
| `outboxer_fatal_after_commit_errors_total` | counter | Fatal sender errors that stop the relay after committing completed work. |
| `outboxer_collect_batch_target` | gauge | The configured `COLLECT_BATCH_TARGET`. |
| `outboxer_last_batch_selected_events` | gauge | Events selected by the most recent committed batch. |
| `outboxer_events_pending_retry` | gauge | Events that failed to send in the most recent batch and remain pending. |
| `outboxer_oldest_event_age_seconds` | gauge | **Outbox lag**: age of the oldest event selected by the most recent batch; `0` when the outbox was empty. Only exposed when `EVENT_TIMESTAMP` is configured. |
| `outboxer_backlog_events` | gauge | **Backlog depth**: this relay's pending events after the last committed batch. See below. |
| `outboxer_backlog_floor` | gauge | `1` when `outboxer_backlog_events` is only a lower bound. |
| `outboxer_last_successful_batch_timestamp_seconds` | gauge | Unix time of the last committed batch (startup time until the first one). |

### What to alert on

- **`outboxer_oldest_event_age_seconds` growing** — the relay is falling
  behind, whatever the cause. This is the SLO metric: backlog *depth* can be
  large while delivery stays fresh, but growing *lag* always means trouble.
- **`time() - outboxer_last_successful_batch_timestamp_seconds` growing** —
  the relay cannot complete its loop at all (database trouble); this is the
  same signal `/healthz` turns into a status code.
- **`rate(outboxer_sender_errors_total[5m])`** — a provider or destination is
  failing; the affected events remain pending and visible in
  `outboxer_events_pending_retry`.

### Backlog depth

`outboxer_backlog_events` is exact whenever a batch **drains** — collects
everything pending for every route it saw — because the events kept for retry
are then the entire remaining backlog, known with no extra query. Only a
truncated batch (some route filled its share of `COLLECT_BATCH_TARGET`) leaves
the depth unknown; the relay then runs a bounded probe:

- The probe scans the oldest `BACKLOG_COUNT_LIMIT` rows (default `100000`) by
  id and counts the ones matching **this relay's** routing predicate — the same
  predicate the collection query uses, so with destination-sharded relays each
  instance reports its own backlog, never another relay's. The scan is bounded
  by the limit regardless of table size or sharding ratio: no `COUNT(*)`, no
  planner statistics.
- It runs at most once per `STATS_INTERVAL_MS` (10s when stats are disabled),
  only after truncated batches, between batches on the relay's own connection.
  A relay with truncated batches never sits in the idle `LISTEN` wait, so the
  probe cannot delay a notification wake-up. `BACKLOG_COUNT_LIMIT=0` disables
  probing entirely.

`outboxer_backlog_floor` is `1` when the reported depth is only a lower bound:
the probe hit its scan cap, probing is disabled while batches are truncated, or
no batch has committed yet. Sustained saturation
(`outboxer_last_batch_selected_events` at the target) with growing lag means
throughput is the bottleneck.

## Health

`/healthz` returns `200` while batches commit, and `503` once no batch has
committed for `HEALTH_STALE_AFTER_MS` (default 5 minutes; `0` disables the
check and always returns `200`). A fresh relay is granted one full window from
startup before a batch is demanded.

The staleness threshold is deliberately generous and the check keys off
*committed batches only*: an idle relay commits an empty batch every poll
cycle, and a batch whose events all failed to send still commits. Provider or
destination failures therefore never flip `/healthz` — they are visible in the
metrics instead. Only the relay's own loop breaking (for example, the database
becoming unreachable) makes it unhealthy, and only after the configured window,
so the scheduler is not restarting the relay for problems a restart cannot fix.

`/` (and every other path) keeps answering an unconditional `200 all good`,
suitable as a pure liveness probe.
