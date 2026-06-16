# Processing test scenarios

Status: draft test plan for the Stage 1 processing redesign.

This file lists behavior scenarios to put in place before the sender pipeline is
rewritten. It is intentionally implementation-facing: each scenario describes
setup, action, and expected outcome. Most scenarios should become table-driven Go
tests with fake Pub/Sub and SQS clients, a fake clock where timing matters, and a
test database or transaction fake where delete/commit behavior matters.

## Test layers

- **Config validation tests**: pure unit tests for startup validation.
- **Routing tests**: pure unit tests for target/backend/destination
  classification.
- **Sender unit tests**: fake provider clients, no real database.
- **Processor orchestration tests**: fake senders plus transaction/delete/commit
  behavior.
- **Provider boundary tests**: fake provider clients that expose batching,
  partial success, delayed/unknown results, and permanent/retryable errors.

Prefer fake clients over live provider tests for Stage 1. Live Pub/Sub/SQS tests
can be added later as optional smoke tests, but they should not be required to
prove the pipeline rules.

## Global invariants

These are cross-cutting assertions that many scenarios should verify.

- A sendable event is deleted only after provider confirmation.
- Content-poison events P3-P7 are included in `done` and removed.
- Routing failures R7/R10-R12 are not included in `done`.
- Retryable send failures are not included in `done`.
- Sender errors do not trigger `ERROR_COOLDOWN`; database/transaction errors do.
- Ordered Pub/Sub keys and SQS FIFO message groups never send a later event while
  an earlier event in the same key/group has an unknown or failed outcome.
- A fatal-after-commit sender error commits already-known `done` events before
  processing stops.

## Configuration

| ID | Scenario | Expected |
| --- | --- | --- |
| CFG-01 | No backend enabled. | Validation fails. |
| CFG-02 | Pub/Sub enabled with no destination column and no default topic. | Validation fails. |
| CFG-03 | SQS enabled with no destination column and no default queue URL. | Validation fails. |
| CFG-04 | Both backends enabled and target column missing. | Validation fails. |
| CFG-05 | `PUBLISH_TIMEOUT_MS` is zero. | Validation fails for Pub/Sub-only, SQS-only, and both-backend configs. |
| CFG-06 | `PUBLISH_TIMEOUT_MS` is negative. | Validation fails. |
| CFG-07 | `PUBLISH_RESULT_GRACE_MS` is negative. | Validation fails. |
| CFG-08 | `ORDERED_GROUP_BATCH_CAP` is zero or negative. | Validation fails. |
| CFG-09 | `SQS_SEND_CONCURRENCY` is zero or negative. | Validation fails. |
| CFG-10 | `WATCHDOG_INTERVAL_MS < 10 * POLL_INTERVAL_MS` when polling is enabled. | Validation fails. |
| CFG-11 | Watchdog interval does not exceed computed `batchSendBound` plus margin. | Validation fails with an error that names the limiting bound. |
| CFG-12 | Watchdog interval equals or exceeds the computed bound plus margin. | Validation succeeds. |

Watchdog bound cases:

- Pub/Sub only: `batchSendBound = ORDERED_GROUP_BATCH_CAP * (PUBLISH_TIMEOUT_MS + PUBLISH_RESULT_GRACE_MS)`.
- SQS standard only: `batchSendBound = ceil(BATCH_SIZE / SQS_SEND_CONCURRENCY) * PUBLISH_TIMEOUT_MS` as a conservative bound for byte-size splits.
- SQS FIFO only: `batchSendBound = max(ceil(BATCH_SIZE / SQS_SEND_CONCURRENCY), ORDERED_GROUP_BATCH_CAP) * PUBLISH_TIMEOUT_MS`.
- Both backends: `batchSendBound` is the max of the enabled backend bounds, not
  their sum.

## Routing

| ID | Scenario | Expected |
| --- | --- | --- |
| ROUTE-01 | Target `pubsub`, Pub/Sub enabled. | Routed to Pub/Sub. |
| ROUTE-02 | Target `sqs`, SQS enabled. | Routed to SQS. |
| ROUTE-03 | Target `pubsub`, Pub/Sub disabled. | Kept as R7; logged through rate limiter. |
| ROUTE-04 | Target `sqs`, SQS disabled. | Kept as R7; logged through rate limiter. |
| ROUTE-05 | Empty target with exactly one backend enabled. | Routed to the enabled backend. |
| ROUTE-06 | Empty target with both backends enabled. | Kept as R11; not `done`. |
| ROUTE-07 | Unknown target such as `kafka`. | Kept as R10; not `done`. |
| ROUTE-08 | Routed event has empty destination and no backend default. | Kept as R12; not `done`. |
| ROUTE-09 | Target names a known disabled backend and destination is also empty. | R7 takes precedence over R12. |

## Batch orchestration

| ID | Scenario | Expected |
| --- | --- | --- |
| BATCH-01 | Pub/Sub and SQS senders both return confirmed events. | Only confirmed IDs are deleted; transaction commits once. |
| BATCH-02 | Sender returns `done` and a non-fatal error. | `done` is deleted and committed; non-`done` events remain; next loop starts without `ERROR_COOLDOWN`. |
| BATCH-03 | Sender returns `done` and fatal-after-commit error. | `done` is deleted and committed; processing stops after commit. |
| BATCH-04 | Delete fails. | Transaction returns error; database cooldown applies; no false "sent" deletion is committed. |
| BATCH-05 | Commit fails after successful send/delete. | Batch returns database error; cooldown applies; next run may duplicate accepted sends. |
| BATCH-06 | Begin/select fails. | No sender is called; cooldown applies. |
| BATCH-07 | Empty selected batch and `POLL_INTERVAL_MS = 0`. | No sleep. |
| BATCH-08 | Empty selected batch and `POLL_INTERVAL_MS > 0`. | Sleeps until poll interval or context cancellation. |
| BATCH-09 | Routing failures only. | No sender is called; nothing is deleted; bounded failure log records the failures. |
| BATCH-10 | Pub/Sub sender and SQS sender both enabled. | They run concurrently; batch time is bounded by the slower backend, not the sum. |

## Failure logging

| ID | Scenario | Expected |
| --- | --- | --- |
| LOG-01 | Same retryable failure repeats in a tight loop. | First occurrence logs immediately; subsequent attempts are suppressed until the rate-limit window summary. |
| LOG-02 | Same event fails with two different signatures. | Each signature has independent rate limiting. |
| LOG-03 | Many events fail for the same destination and reason. | Logs aggregate by signature and include suppressed count. |
| LOG-04 | Context cancellation causes expected shutdown errors. | No noisy failure log for cancellation fallout. |

## Pub/Sub happy paths

| ID | Scenario | Expected |
| --- | --- | --- |
| PS-OK-01 | Unordered events for one topic all succeed. | All event IDs returned in `done`; publisher is flushed before waiting on results. |
| PS-OK-02 | Unordered events for multiple topics all succeed. | One cached publisher per topic; all successful IDs returned in `done`. |
| PS-OK-03 | Ordered events for one key all succeed. | Events are published one at a time in input order; each next publish happens only after prior success. |
| PS-OK-04 | Ordered events for multiple keys all succeed. | Each key is sequential internally; different keys may progress concurrently. |
| PS-OK-05 | Ordered and unordered events are mixed. | Ordered keys preserve order; unordered events may batch normally; all successes returned in `done`. |
| PS-OK-06 | More than `ORDERED_GROUP_BATCH_CAP` events for one ordering key are selected. | Only capped count is attempted; remainder is kept for a later batch. |
| PS-OK-07 | Publisher already exists for topic. | Sender reuses cached publisher and does not recreate it. |
| PS-OK-08 | Sender closes. | Each cached publisher receives `Stop()` exactly once. |

## Pub/Sub local prevalidation

Boundary values should be table-driven with "just below", "exactly at", and
"just above" where the provider limit is inclusive.

| ID | Scenario | Expected |
| --- | --- | --- |
| PS-PRE-01 | Data exceeds `10_000_000` bytes. | P3 content-poison; event returned in `done`; no publish call. |
| PS-PRE-02 | Data equals `10_000_000` bytes. | Not locally poison by data size alone. |
| PS-PRE-03 | Single event cannot fit into a 10 MB publish request. | P3 only if exact encoded size proves it or isolated backend reject confirms it. |
| PS-PRE-04 | Multi-event publish request would exceed 10 MB. | Split request; no event is poison for multi-event overflow alone. |
| PS-PRE-05 | More than 1000 messages would be sent in one publish request. | Split request. |
| PS-PRE-06 | More than 100 sanitized string attributes. | P5; no publish call. |
| PS-PRE-07 | Attribute key length exceeds 256 bytes. | P5; no publish call. |
| PS-PRE-08 | Attribute value length exceeds 1024 bytes. | P5; no publish call. |
| PS-PRE-09 | Attribute key starts with `goog`. | P5; no publish call. |
| PS-PRE-10 | Non-string attributes are present. | Non-string attributes are dropped and logged; string attributes are still validated. |
| PS-PRE-11 | Empty data, no attributes, no ordering key. | P4; no publish call. |
| PS-PRE-12 | Empty data, no attributes, ordering key present. | Not local poison; publish in isolation before classification. |
| PS-PRE-13 | Bare topic ID is syntactically valid. | Accepted for publish. |
| PS-PRE-14 | Full `projects/{project}/topics/{topic}` name is syntactically valid. | Accepted for publish. |
| PS-PRE-15 | Topic ID too short, starts with non-letter, starts with `goog`, or has invalid characters. | P7; no publish call. |
| PS-PRE-16 | Topic syntactically valid but provider returns not found. | R4; event kept. |

## Pub/Sub provider failures

| ID | Scenario | Expected |
| --- | --- | --- |
| PS-FAIL-01 | Unordered publish returns retryable error. | Event omitted from `done`; sender returns non-fatal error or logs/rate-limits per design. |
| PS-FAIL-02 | Ordered publish returns retryable error. | Failed event and later same-key events are kept; `ResumePublish(key)` is called before future sends for that key. |
| PS-FAIL-03 | Ordered key has failure after earlier successes. | Earlier successes are in `done`; failed event and later same-key events are not sent/deleted. |
| PS-FAIL-04 | Bundle returns permanent error for multiple unordered events. | No event is deleted solely from bundle error; failed bundle enters single-event isolation. |
| PS-FAIL-05 | Isolation identifies one permanent bad event and one valid event. | Bad event returned in `done` as poison; valid event is sent/confirmed or kept according to its isolated result. |
| PS-FAIL-06 | Isolation returns retryable error. | Event is kept; no poison deletion. |
| PS-FAIL-07 | Ordered isolation hits first non-done event. | That event and later same-key events are kept; no later same-key publish occurs. |
| PS-FAIL-08 | Ordered `Get` still times out after provider timeout plus grace. | Sender returns fatal-after-commit error; no later event for that key is published in the process. |
| PS-FAIL-09 | Unordered `Get` still times out after provider timeout plus grace. | Event omitted from `done`; sender continues; duplicate on later retry is allowed. |
| PS-FAIL-10 | `Get` context timeout occurs before provider timeout plus grace due to wrong wait bound. | Test should fail; implementation must not use too-short caller wait. |
| PS-FAIL-11 | Publisher enters unrecoverable internal state. | Sender returns fatal-after-commit error; no in-process publisher recreation. |
| PS-FAIL-12 | Context is canceled during publish wait. | In-flight unconfirmed event is omitted from `done`; shutdown proceeds. |

## Pub/Sub batching and flushing

| ID | Scenario | Expected |
| --- | --- | --- |
| PS-BATCH-01 | Unordered batch is enqueued. | `Flush()` is called before any result wait. |
| PS-BATCH-02 | Ordered key publishes one event. | `Flush()` is called before waiting for that event's result. |
| PS-BATCH-03 | Multiple ordered keys are active. | Per-key flushes do not require synchronized rounds; slow key does not block another key's next event after its own prior success. |
| PS-BATCH-04 | Publisher delay threshold is high in fake client. | Explicit `Flush()` prevents artificial batch delay. |

## SQS happy paths

| ID | Scenario | Expected |
| --- | --- | --- |
| SQS-OK-01 | Standard queue with 1 event. | One `SendMessageBatch` request with one entry; success returned in `done`. |
| SQS-OK-02 | Standard queue with 10 events. | One batch request with 10 entries. |
| SQS-OK-03 | Standard queue with 11 events. | Two batch requests, 10 + 1. |
| SQS-OK-04 | Standard queue with many events and concurrency 2. | Batch requests run in at most 2 concurrent waves. |
| SQS-OK-05 | FIFO queue with one message group and 3 events. | Three single-message sends in order; no multi-entry batch for that group. |
| SQS-OK-06 | FIFO queue with two message groups. | Each group is sequential internally; groups may run concurrently under the global semaphore. |
| SQS-OK-07 | FIFO group has more than `ORDERED_GROUP_BATCH_CAP` events. | Only capped count is attempted; remainder is kept. |
| SQS-OK-08 | FIFO event has no ordering key. | Stable provider-safe synthetic `MessageGroupId` is derived from event ID. |
| SQS-OK-09 | FIFO event ID is valid as dedup ID. | Raw event ID may be used as `MessageDeduplicationId`. |
| SQS-OK-10 | FIFO event ID is too long or has invalid characters. | Stable collision-resistant digest is used as provider-safe dedup ID. |
| SQS-OK-11 | Standard batch entry ID would be invalid if raw event ID were used. | Stable provider-safe batch entry ID is derived. |

## SQS local prevalidation

Boundary values should cover body size, attribute size, attribute count, and FIFO
identifier lengths.

| ID | Scenario | Expected |
| --- | --- | --- |
| SQS-PRE-01 | Message body plus attributes exceeds 1 MiB. | P3; no provider call. |
| SQS-PRE-02 | Standard batch total exceeds 1 MiB but individual messages fit. | Split batch; no event is poison for batch overflow alone. |
| SQS-PRE-03 | Standard batch has more than 10 entries. | Split into chunks of 10. |
| SQS-PRE-04 | Body is empty. | P4; no provider call. |
| SQS-PRE-05 | Body contains unsupported Unicode. | P4; no provider call. |
| SQS-PRE-06 | Body contains allowed boundary characters. | Accepted for publish. |
| SQS-PRE-07 | More than 10 sanitized string attributes. | P5; no provider call. |
| SQS-PRE-08 | Attribute name is invalid: empty, starts with `AWS.`/`Amazon.`, starts/ends with `.`, has `..`, or contains invalid chars. | P5; no provider call. |
| SQS-PRE-09 | Attribute name or type exceeds 256 chars. | P5; no provider call. |
| SQS-PRE-10 | Attribute value contains unsupported Unicode. | P4 or P5 as implemented, but must be local poison and not provider retry. |
| SQS-PRE-11 | FIFO ordering key exceeds 128 chars. | P6; no provider call. |
| SQS-PRE-12 | FIFO ordering key contains invalid characters. | P6; no provider call. |
| SQS-PRE-13 | Queue URL is syntactically invalid. | P7; no provider call. |
| SQS-PRE-14 | Queue URL is syntactically valid but provider returns not found. | R4; event kept. |

## SQS provider failures

| ID | Scenario | Expected |
| --- | --- | --- |
| SQS-FAIL-01 | Standard batch returns partial success. | Successful entries returned in `done`; retryable failed entries kept. |
| SQS-FAIL-02 | Standard batch returns `SenderFault=true` for one entry. | That entry returned in `done` as content-poison; other successful entries deleted; retryable failures kept. |
| SQS-FAIL-03 | Standard batch API call returns retryable error for whole request. | No entries from that request are `done`; events kept. |
| SQS-FAIL-04 | Standard batch API call returns permanent request error caused by a single invalid event. | Sender isolates or uses local proof before deleting any event as poison. |
| SQS-FAIL-05 | FIFO same group event 1 succeeds, event 2 retryable fails, event 3 exists. | Event 1 is `done`; events 2 and 3 are kept; event 3 is not sent this batch. |
| SQS-FAIL-06 | FIFO same group event 1 is content-poison, event 2 exists. | Event 1 is `done` as poison; event 2 may be sent after event 1 is classified done. |
| SQS-FAIL-07 | FIFO same group event 1 has unknown/timeout result. | Event 1 and later same-group events are kept; no later same-group send occurs. |
| SQS-FAIL-08 | FIFO different groups include one failing group and one successful group. | Successful independent group events can be `done`; failing group remainder is kept. |
| SQS-FAIL-09 | Queue not found. | R4; events kept; bounded log. |
| SQS-FAIL-10 | Permission denied. | R5; events kept; bounded log. |
| SQS-FAIL-11 | Auth/credential failure. | R6; events kept; SDK credential refresh is relied on. |
| SQS-FAIL-12 | Throttling. | R3; events kept; sender duration is paced by SDK retry and publish timeout. |
| SQS-FAIL-13 | Context canceled during send. | Unconfirmed events kept; shutdown proceeds. |

## Ordering across batches

| ID | Scenario | Expected |
| --- | --- | --- |
| ORDER-01 | Batch 1 selects ordered events 1-8 for a key; cap is 8; batch 2 selects 9-16. | Provider sees 1-16 in order across batches if all succeed. |
| ORDER-02 | Batch 1 ordered event 4 fails retryably. | Batch 1 sends 1-4 only for that key; 5-8 are kept. Batch 2 cannot send 5 before 4 succeeds. |
| ORDER-03 | Ordered event succeeds at provider but transaction commit fails. | Next run may duplicate that event, but must not delete a later event ahead of it. |
| ORDER-04 | Two Outboxer instances run concurrently. | Second instance blocks on `FOR UPDATE`; it does not process the same ordered queue concurrently. |

## Shutdown and lifecycle

| ID | Scenario | Expected |
| --- | --- | --- |
| LIFE-01 | Normal shutdown before selecting batch. | No sender call; exits cleanly. |
| LIFE-02 | Shutdown during sender work. | Context cancellation reaches senders; unconfirmed events kept. |
| LIFE-03 | Fatal-after-commit error occurs. | Known `done` events are committed, then processing stops and process supervisor restarts. |
| LIFE-04 | `Close()` called after processing stops. | Pub/Sub publishers are stopped; SQS client is not recreated during processing. |
| LIFE-05 | Client would otherwise be recreated on persistent error. | Test ensures implementation returns fatal-after-commit or retryable error instead of creating a second in-process publisher/client. |

## Database and deletion behavior

| ID | Scenario | Expected |
| --- | --- | --- |
| DB-01 | `DELETE` is called with empty `done`. | No invalid SQL; transaction still commits selected-but-kept events. |
| DB-02 | `DELETE` includes duplicate IDs from sender result. | Delete set is de-duplicated or deletion remains correct. |
| DB-03 | `done` contains content-poison and confirmed sent events. | Both are deleted in the same Stage 1 commit. |
| DB-04 | Sender marks an ID not present in selected batch. | Implementation rejects or ignores it safely; no unrelated row deletion. |
| DB-05 | Commit succeeds after partial provider success. | Only provider-success/content-poison events are deleted. |
| DB-06 | Rollback path after batch error. | Rollback is attempted/logged when appropriate and does not mask original error. |

## Metrics and observability

These may be added as tests once metrics exist.

- Oldest event age by destination/target.
- Count of routing failures by classification R7/R10/R11/R12.
- Count of content-poison removals by P3-P7.
- Count of retryable provider failures by R1-R6/R9.
- Count of suppressed logs by failure signature.
- Publish latency by backend and destination.
- Pub/Sub unknown ordered-result fatal count.
- SQS partial-success count.

## Suggested implementation order

1. Config and routing tests.
2. Provider prevalidation tests.
3. SQS sender tests, because SQS has explicit per-entry fake responses.
4. Pub/Sub sender tests with fake async results and fake flush/stop tracking.
5. Batch orchestration tests with fake senders.
6. Failure-log rate-limit tests with a fake clock.
7. End-to-end processor tests around a real test database transaction.

The scenario list is deliberately larger than the first implementation PR needs.
Start with the P0 safety paths: no loss, ordered failure stops, Pub/Sub unknown
ordered result is fatal-after-commit, SQS FIFO never batches same-group events,
content poison is deleted only after local proof or single-event isolation, and
routing failures are kept.
