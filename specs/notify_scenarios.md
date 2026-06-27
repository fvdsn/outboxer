# LISTEN/NOTIFY Wake-up Scenarios

Status: implementation test plan.

These scenarios accompany [`notify_requirements.md`](notify_requirements.md).
They focus on preserving the existing polling guarantees while adding the
notification fast-path, and on never losing an event when a notification is
missed. Layer hints: most config and wake-up behavior is integration-testable
against real Postgres; the trigger fast-path and fallback latency are e2e smoke
tests against the real binary.

## Configuration & Activation

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-CFG-01 | `POLL_INTERVAL_MS=0` (default). | No `LISTEN` is issued; hot-loop polling; behavior byte-for-byte identical to today. |
| NTF-CFG-02 | `POLL_INTERVAL_MS>0`, default channel. | `LISTEN outboxer_events` is issued exactly once on the pinned connection. |
| NTF-CFG-03 | `NOTIFY_CHANNEL` set to a custom value. | `LISTEN` uses the custom channel. |
| NTF-CFG-04 | No dedicated enable/disable flag is added. | Behavior is keyed only off `POLL_INTERVAL_MS`; there is no separate toggle. |
| NTF-CFG-05 | `POLL_INTERVAL_MS>0` with `WATCHDOG_INTERVAL_MS` < 10x interval. | Startup validation fails, as today. |
| NTF-CFG-06 | `NOTIFY_CHANNEL` empty while `POLL_INTERVAL_MS>0`. | Either the default is applied or startup fails; never `LISTEN` on an empty channel. |
| NTF-CFG-07 | `NOTIFY_CHANNEL` contains characters needing quoting. | The channel name is safely quoted in the `LISTEN` statement. |

## Wake-up With Trigger Present

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-WAKE-01 | Idle process, large interval (e.g. 30s), one insert via trigger. | The process wakes and delivers the event well before the interval elapses. |
| NTF-WAKE-02 | Bulk insert of many rows in one statement (statement-level trigger). | A single notification wakes the process; all rows are drained. |
| NTF-WAKE-03 | Burst of separate inserts during one wait. | Buffered notifications are drained and collapsed into one (or few) cycles; no per-insert wake storm. |
| NTF-WAKE-04 | Notification fires while a batch is being processed (no session is listening then). | Processing continues batch-to-batch until a batch is empty, so the row is picked up by a subsequent select; the in-processing notification is not relied upon and the row is never missed. |
| NTF-WAKE-05 | Continuous insert stream. | The process stays in the drain loop (busy), not repeatedly returning to wait; throughput is unaffected. |
| NTF-WAKE-06 | Notification delivered on `COMMIT` (transactional). | The process is woken only after the inserted rows are visible; it never wakes to find nothing inserted yet. |

## Sweep / Backstop

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-SWEEP-01 | `POLL_INTERVAL_MS>0`, no trigger installed, steady inserts. | Delivery latency is bounded by the interval (today's polling behavior). |
| NTF-SWEEP-02 | Trigger present, notification fires during a reconnect window (no listener). | The event is still delivered by the next sweep; no event is lost. |
| NTF-SWEEP-03 | Long idle period with no notifications. | The sweep fires every interval; the process stays alive; the watchdog is satisfied. |
| NTF-SWEEP-04 | `NOTIFY_CHANNEL` does not match the trigger's channel. | No wake-ups arrive; delivery falls back to the sweep interval; still correct. |

## Connection Model

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-CONN-01 | `POLL_INTERVAL_MS>0` steady state. | Exactly one DB connection is used (`MaxOpenConns(1)`); the listener borrows it only while idle and releases it before the next batch. |
| NTF-CONN-02 | The connection drops mid-wait. | The wait surfaces an error; the next idle cycle borrows a fresh connection and re-`LISTEN`s; processing resumes; the sweep covers the gap; no crash. |
| NTF-CONN-03 | Statistics logging fires while the listener is mid-wait. | The stats logger reads the cached estimate without a database query, so it neither blocks nor needs a second connection. |
| NTF-CONN-04 | Postgres fronted by a transaction-pooling pooler (e.g. pgbouncer). | Documented as unsupported for `LISTEN`; the direct single connection is unaffected. |

## Statistics Estimate

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-STATS-01 | The processor refreshes the backlog estimate. | The estimate is queried on the processor connection and cached; `events_remaining_estimate` reflects it. |
| NTF-STATS-02 | Two refreshes occur within one `STATS_INTERVAL_MS`. | Only one query runs; the cached value is reused (throttled). |
| NTF-STATS-03 | The estimate query is unavailable. | The cached value reports unavailable and the stats field is omitted. |

## Watchdog

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-WD-01 | Long idle with `POLL_INTERVAL_MS>0` and no events. | Each sweep wake marks processor progress; the deadlock detector does not trigger. |
| NTF-WD-02 | `WaitForNotification` blocks for the full interval repeatedly. | Progress is still marked within the watchdog window (10x rule). |

## Shutdown

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-SHUT-01 | `SIGTERM` during `WaitForNotification`. | The wait returns promptly via context cancellation; graceful shutdown completes; exit `0`. |
| NTF-SHUT-02 | `SIGTERM` while draining a burst. | The in-flight batch follows existing shutdown semantics; the process exits cleanly. |

## End-To-End Smoke Tests

| ID | Scenario | Expected |
| --- | --- | --- |
| NTF-E2E-01 | Real binary + Postgres + trigger installed + emulator, large interval. | An inserted event is published with low latency, well under the interval. |
| NTF-E2E-02 | Real binary + Postgres, trigger NOT installed, interval set. | An inserted event is delivered within approximately the interval. |
| NTF-E2E-03 | Real binary, trigger installed, kill -9 and restart mid-idle. | After restart the process re-`LISTEN`s and resumes; no event is lost. |
