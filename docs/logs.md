# Logging

Outboxer logs to stdout using Go's `log/slog`. The level is set with `LOG_LEVEL`
(`debug`, `info`, `warn`, `error`; default `info`) and the format with
`LOG_FORMAT` (`text`, the default human-readable output, or `json` for log
aggregators). See [Configuration](configuration.md#logging).

Outboxer uses `debug` for high-volume per-event and heartbeat detail, `info` for
lifecycle and batch progress, `warn` for recoverable anomalies, and `error` for
failures. The default `info` level stays quiet under load because per-event logs
are `debug`.

## Log catalog

### Lifecycle (`info`)

| Message | When | Key fields |
| --- | --- | --- |
| `Startup` | Process starts. | `pid` |
| `Health server listening` | Health server bound, when a port is configured. | `port` |
| `Processing events` | Processing loop starts. | `table` |
| `Processing batch` | A non-empty batch is selected. | `count` (rows in the batch) |
| `Statistics` | Every `STATS_INTERVAL_MS`. | See [Statistics](#statistics). |
| `Graceful shutdown` | `SIGINT` / `SIGTERM` received. | — |

### Debug detail (`debug`)

| Message | When | Key fields |
| --- | --- | --- |
| `Sending event` | A single event is about to be published. | `event_id`, `event_timestamp`, `event_latency`, `event_payload_size`, `event_target`, `event_destination`, and (Pub/Sub) `event_ordering_key`, `event_attributes` |
| `Event sent` | A publish is confirmed by the backend. | `event_id`, `event_published_id`, `event_destination`, `publish_latency` |
| `Watchdog heartbeat` | Each watchdog tick while healthy. | — |
| `Healthcheck request answered` | A health request returns `200`. | — |
| `Notification wake-ups enabled` | Logged once at startup when `POLL_INTERVAL_MS > 0`. | `channel` |
| `Failed to start notification listener, polling instead` | The listener could not be started this idle cycle; it falls back to a plain sweep. | `error` |
| `Notification wait failed, polling next cycle` | The listener connection failed mid-wait; the next idle cycle tries again. | `error` |

`event_latency` is the time between the event `timestamp` column and when
Outboxer picked it up (only meaningful when `EVENT_TIMESTAMP` is configured).
`publish_latency` is the time spent in the provider publish call.

### Anomalies (`warn`)

| Message | When | Key fields |
| --- | --- | --- |
| `Some attributes were dropped` | Non-string Pub/Sub attributes were discarded before publishing; the event is still sent. | `event_id`, `event_destination`, `dropped_attributes` |

### Failures (`error`)

| Message | When | Key fields |
| --- | --- | --- |
| `Failed to send event` | A single event failed to publish. | `event_id`, `event_destination`, `error` |
| `Failed to send event batch` | A batched send call failed. | `error` |
| `Event cannot be routed, leaving it in the table` | An event has no valid route (unknown/disabled target, missing destination). | `event_id`, `error` |
| `Sender reported an ID outside the selected batch, ignoring it` | A backend returned a result for an unexpected id. | `event_id` |
| `Failed to start batch transaction` | Opening the batch transaction failed. | `error` |
| `Failed during batch transaction` | A select/send/delete step failed; the batch is rolled back. | `error` |
| `Failed to rollback batch transaction` | The rollback itself failed. | `error` |
| `Failed to commit batch transaction` | The commit failed. | `error` |
| `Deadlock detected, shutting down` | The watchdog detected a stalled processor. | — |
| `Fatal sender error after commit, stopping processor` | An unrecoverable sender error occurred after commit. | `error` |
| `Failed to close Pub/Sub publisher` | Cleanup error during shutdown. | `error` |
| `HTTP server failed` | The health server stopped unexpectedly. | `error` |
| `Fatal error` | Top-level fatal error; the process then exits non-zero. | `error` |

Batch-transaction errors are suppressed when they are just the expected fallout
of a shutdown (the context is already cancelled), so a clean shutdown does not
produce spurious error logs.

#### Repeated-failure rate limiting

`Failed to send event`, `Failed to send event batch`, the routing failure, and
the out-of-batch-id messages go through a rate limiter keyed by failure
signature. Identical failures are logged at most once per minute; when a burst
is collapsed, the next emitted log carries a `suppressed_count` field with the
number of occurrences that were skipped. This keeps a sustained provider outage
from flooding the logs while still surfacing the error and its true rate.

## Statistics

When `STATS_INTERVAL_MS` is positive (default `10000`), Outboxer logs a periodic
`Statistics` record at `info` level. Each field except `stats_interval_ms` is a
**counter for the interval that just elapsed**; counters reset to zero after
every record. They are per-interval deltas, not running totals.

| Field | Type | Meaning |
| --- | --- | --- |
| `stats_interval_ms` | gauge | The configured interval, echoed for context. |
| `events_selected` | counter | Rows selected from the outbox table this interval. |
| `events_sent` | counter | Events confirmed published to a backend. |
| `events_poison` | counter | Events classified as permanently unsendable (content poison). |
| `events_dlq` | counter | Poison events written to the DLQ table. A subset of `events_poison`: equal to it when `DLQ_TABLE` is set, otherwise `0` (poison is deleted instead). |
| `events_kept_for_retry` | counter | Events left in the outbox table for a later retry — routing failures and retryable provider failures. |
| `batches_processed` | counter | Batches committed this interval. |
| `batch_errors` | counter | Batches that failed and triggered a database cooldown. |
| `sender_errors` | counter | Per-event sender errors observed during batches. |
| `fatal_after_commit_errors` | counter | Fatal sender errors that stopped the processor after commit. |

### Interpreting the numbers

- Within an interval, selected events end up sent, dead-lettered/deleted as
  poison, or kept for retry, so `events_selected` is approximately
  `events_sent + events_poison + events_kept_for_retry` (batch and interval
  boundaries do not align exactly, so it is not an exact identity).
- A healthy idle Outboxer logs all-zero counters. Steady
  `events_kept_for_retry` with low `events_sent` points at a routing or provider
  problem; cross-reference the `error`-level logs above.
- Non-zero `batch_errors` means the loop is hitting the database cooldown; a
  non-zero `fatal_after_commit_errors` means the processor stopped and the
  supervisor should have restarted it.
