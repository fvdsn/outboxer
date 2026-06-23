# Processing use cases and test scenarios

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
| CFG-08 | `SQS_SEND_CONCURRENCY` is zero or negative. | Validation fails. |
| CFG-09 | `WATCHDOG_INTERVAL_MS < 10 * POLL_INTERVAL_MS` when polling is enabled. | Validation fails. |
| CFG-10 | `COLLECT_BATCH_TARGET` is zero or negative. | Validation fails. |
| CFG-11 | `PUBSUB_DESTINATIONS` is set while Pub/Sub is disabled. | Validation fails. |
| CFG-12 | `SQS_DESTINATIONS` is set while SQS is disabled. | Validation fails. |
| CFG-13 | `EVENT_OPTIONS` is unset. | Options column defaults to `options`. |
| CFG-14 | `EVENT_OPTIONS` is empty. | Backend-specific options are disabled; every event behaves as if options were `{}`. |

Watchdog bound cases:

- The selected count is data-dependent; watchdog tests should keep the watchdog
  alive during long valid batches instead of relying on a static startup bound.
- Pub/Sub concurrency is bounded by publisher flow control and ordered-key
  sequencing; `SQS_SEND_CONCURRENCY` affects SQS only.
- Both backends: the send bound is the max of the enabled backend bounds, not
  their sum.

## Routing

| ID | Scenario | Expected |
| --- | --- | --- |
| ROUTE-01 | Target `pubsub`, Pub/Sub enabled. | Routed to Pub/Sub. |
| ROUTE-02 | Target `sqs`, SQS enabled. | Routed to SQS. |
| ROUTE-03 | Target `pubsub`, Pub/Sub disabled. | Classified as R7; not `done`. |
| ROUTE-04 | Target `sqs`, SQS disabled. | Classified as R7; not `done`. |
| ROUTE-05 | Empty target with exactly one backend enabled. | Routed to the enabled backend. |
| ROUTE-06 | Empty target with both backends enabled. | Kept as R11; not `done`. |
| ROUTE-07 | Unknown target such as `kafka`. | Kept as R10; not `done`. |
| ROUTE-08 | Routed event has empty destination and no backend default. | Kept as R12; not `done`. |
| ROUTE-09 | Target names a known disabled backend and destination is also empty. | R7 takes precedence over R12. |
| ROUTE-10 | `target` is empty, both backends are enabled, and `options` contains only `pubsub`. | Kept as R11; target is not inferred from options. |
| ROUTE-11 | `target=pubsub` and `options` also contains an `sqs` section. | Routed to Pub/Sub; non-selected backend options are ignored. |

## Collection

These scenarios test selection before sender routing. Use a real database query
or a close SQL-level fake; this is where ordering, limits, and projected columns
matter.

| ID | Scenario | Expected |
| --- | --- | --- |
| COLLECT-01 | 10 routes with 10 valid events each and `COLLECT_BATCH_TARGET=100`. | Selects all 100 events in one batch, ordered by id within each route. |
| COLLECT-02 | One route has 100 valid events and `COLLECT_BATCH_TARGET=40`. | Selects the first 40 events for that route; later events remain for later batches. |
| COLLECT-03 | 100 old valid events for route A and 10 newer valid events for route B, `COLLECT_BATCH_TARGET=40`. | Computes an effective cap of 20 per route, selects 20 from route A and 10 from route B; route A cannot occupy the whole selected batch. |
| COLLECT-04 | The table has only R7/R10/R11/R12 routing failures. | Selects zero rows; no sender is called and no routing-failure log is emitted by the processor. |
| COLLECT-05 | The table has routing failures plus valid routes. | Selects only events that resolve to enabled backends with non-empty destinations; routing failures remain pending. |
| COLLECT-06 | Empty targets with exactly one backend enabled. | Empty target events are eligible and grouped under that enabled backend. |
| COLLECT-07 | Empty destinations and a backend default. | Events are eligible and grouped under the resolved default destination. |
| COLLECT-08 | Empty destinations and no backend default. | Events are not eligible; they remain pending as R12. |
| COLLECT-09 | Explicit destination `D` and empty destination resolving to default `D`. | Both forms share the same resolved route and therefore the same computed per-route cap. |
| COLLECT-10 | Collection discovers routes and selects route rows using synthetic `resolved_target` / `resolved_destination` values. | Final selected rows contain only base event-table columns; synthetic columns do not leak into `event.columns`. |
| COLLECT-11 | Many valid routes have pending events. | Selected count is at most `eligible_route_count * ceil(COLLECT_BATCH_TARGET / eligible_route_count)`, with every eligible route getting at least one slot. |
| COLLECT-12 | Two Outboxer instances select concurrently. | The first transaction locks selected rows with `FOR UPDATE`; the second blocks on those rows instead of processing the same events. |
| COLLECT-13 | `PUBSUB_DESTINATIONS=topic-a,topic-b` and the table contains Pub/Sub events for `topic-a`, `topic-b`, and `topic-c`. | Selects only `topic-a` and `topic-b`; `topic-c` remains pending and is not logged as a routing failure by this process. |
| COLLECT-14 | `SQS_DESTINATIONS=queue-a` and the table contains SQS events for `queue-a` and `queue-b`. | Selects only `queue-a`; `queue-b` remains pending and is not logged as a routing failure by this process. |
| COLLECT-15 | Empty destination resolves to default destination `D`, and `D` is included in the backend destination allowlist. | The event is eligible and grouped with explicit destination `D`. |
| COLLECT-16 | Two Outboxer instances on the same table have disjoint destination allowlists. | Each instance selects only its owned destination routes, so they can process without blocking on each other's rows. |
| COLLECT-17 | Two Outboxer instances on the same table have overlapping destination allowlists. | Overlapping selected rows are still serialized by `FOR UPDATE`; they must not process the same ordered route concurrently. |
| COLLECT-18 | Events include different `options` values but the same resolved target and destination. | They share the same resolved route and collection cap; options do not affect route grouping. |
| COLLECT-19 | Events have `options.pubsub.topic` or `options.sqs.queueUrl` values but empty `destination`. | Options destinations are ignored; events use backend defaults or remain R12. |

## Options

| ID | Scenario | Expected |
| --- | --- | --- |
| OPT-01 | Options column is absent or disabled. | Backend-specific options resolve to `{}`; sends use no explicit attributes or ordering/group key. |
| OPT-02 | Options column is `NULL`. | Backend-specific options resolve to `{}`. |
| OPT-03 | Options value is not a JSON object. | Selected event is content-poison P5 and is returned in `done` without a provider call. |
| OPT-04 | Selected backend section is missing or `null`. | Backend-specific options resolve to `{}` for that backend. |
| OPT-05 | Selected backend section is not a JSON object. | Selected event is content-poison P5 and is returned in `done` without a provider call. |
| OPT-06 | `options.pubsub.orderingKey` is a string. | Pub/Sub sender uses it as the ordering key. |
| OPT-07 | `options.pubsub.orderingKey` is non-string. | Event is content-poison P6; no provider call is made. |
| OPT-08 | `options.sqs.messageGroupId` is a string for a FIFO queue. | SQS sender uses it as the `MessageGroupId`. |
| OPT-09 | `options.sqs.messageGroupId` is non-string. | Event is content-poison P6; no provider call is made. |
| OPT-10 | `options.pubsub.attributes` is an object with string values. | Pub/Sub message attributes are set from that object. |
| OPT-11 | `options.sqs.attributes` is an object of native AWS `MessageAttributeValue` JSON objects. | SQS message attributes are set from that object, including `DataType`, `StringValue`, and base64 `BinaryValue`. |
| OPT-12 | Backend attributes option is non-object. | Event is content-poison P5; no provider call is made. |
| OPT-13 | Pub/Sub attributes object contains non-string values. | Non-string values are dropped and logged; string values are still sent and validated. |
| OPT-14 | Options contain unknown keys. | Unknown keys are ignored and do not make the event poison. |
| OPT-15 | Event row still has legacy `ordering_key` and `attributes` columns. | They are ignored; only `options` supplies backend metadata. |
| OPT-16 | `destination` column is present alongside options. | `destination` remains the only per-event destination source; options do not override it. |
| OPT-17 | `options.sqs.messageGroupId` is set for a standard queue. | SQS sender passes it as `MessageGroupId` without changing standard queue batching or ordering behavior. |
| OPT-18 | `options.sqs.messageDeduplicationId` is set for a FIFO queue. | SQS sender uses it instead of the event-id-derived deduplication ID. |
| OPT-19 | `options.sqs.messageDeduplicationId` is invalid. | Event is content-poison P6; no provider call is made. |
| OPT-20 | `options.sqs.delaySeconds` is an integer from 0 to 900 on a standard queue. | SQS sender passes it as `DelaySeconds`. |
| OPT-21 | `options.sqs.delaySeconds` is set on a FIFO queue. | SQS sender does not send per-message delay for FIFO queues. |
| OPT-22 | `options.sqs.delaySeconds` is non-integer or outside 0-900. | Event is content-poison P6; no provider call is made. |
| OPT-23 | `options.sqs.messageSystemAttributes.AWSTraceHeader` is set. | SQS sender sends it as the `AWSTraceHeader` message system attribute. |
| OPT-24 | `options.sqs.attributes` contains shorthand string values. | Event is content-poison P5; SQS shorthand attributes are not supported. |
| OPT-25 | `options.sqs.attributes` contains `StringListValues` or `BinaryListValues`. | Event is content-poison P5; list values are reserved by SQS and not sent. |

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
| BATCH-10 | Pub/Sub sender and SQS sender both enabled. | They run concurrently; batch time is bounded by the slower backend, not the sum. |
| BATCH-11 | The table contains only routing failures. | No rows are selected; no sender is called; nothing is deleted; no routing-failure log is emitted by the processor. |
| BATCH-12 | Collection selects a valid route whose provider fast-fails and another valid route that succeeds. | Successful route events are deleted at commit; failed route events remain. The failed route may slow the batch until bounded sender operations return, but it does not prevent selection of the other route. |
| BATCH-13 | Sender returns `done` and fatal-after-commit error, then delete fails. | Delete/transaction error is logged, but processing still stops; it must not loop in the same process after an unknown ordered outcome. |
| BATCH-14 | Sender returns `done` and fatal-after-commit error, delete succeeds, then commit fails. | Commit error is logged, restart may duplicate provider-accepted events, and processing still stops; it must not continue behind the unknown ordered outcome. |

## Realistic happy-path batches

These scenarios are intentionally larger than the focused unit cases. They should
catch mistakes in routing, grouping, chunking, delete accounting, defaults, and
provider-call shape before live integration tests exist.

Each scenario names any non-default limits needed to make the selected-batch
size unambiguous.

| ID | Scenario | Expected |
| --- | --- | --- |
| REAL-OK-01 | SQS-only standard queue, 100 events, one queue, `COLLECT_BATCH_TARGET=100`. | Selects 100 events for the one route; exactly 10 `SendMessageBatch` calls of 10 entries each; all 100 IDs are `done`. |
| REAL-OK-02 | SQS-only standard queue, 100 events split across 10 queue URLs, 10 per queue, default `COLLECT_BATCH_TARGET=5000`. | Selects all 100 events; each queue receives one batch of 10; the global SQS semaphore is respected across queues; all IDs are `done`. |
| REAL-OK-03 | Pub/Sub-only unordered, 100 events split across 10 topics, 10 per topic, default `COLLECT_BATCH_TARGET=5000`. | Selects all 100 events; one cached publisher per topic; each topic is flushed; all IDs are `done`; no topic receives another topic's messages. |
| REAL-OK-04 | Both backends enabled, 100 events mixed across 5 Pub/Sub topics and 5 SQS standard queues, 10 per destination, default `COLLECT_BATCH_TARGET=5000`. | Selects all 100 events; messages are grouped by backend and destination; every destination receives exactly its intended 10 events; all selected IDs are deleted. |
| REAL-OK-05 | Pub/Sub-only with no target column and a default topic, 100 events with empty destination, `COLLECT_BATCH_TARGET=100`. | All events route to Pub/Sub default topic, publish successfully, and are deleted. |
| REAL-OK-06 | SQS-only with no target column and a default standard queue URL, 100 events with empty destination, `COLLECT_BATCH_TARGET=100`. | All events route to the default queue, send as 10 standard batches, and are deleted. |
| REAL-OK-07 | Both backends enabled with explicit targets and a mix of explicit destinations plus backend defaults. | Empty Pub/Sub destinations use the Pub/Sub default, empty SQS destinations use the SQS default, explicit destinations are preserved, and all valid events are deleted according to each route's cap. |
| REAL-OK-08 | Mixed Pub/Sub unordered and Pub/Sub ordered events for multiple topics and keys, all successful. | Unordered events batch/flush by topic; ordered events remain sequential per `(destination, options.pubsub.orderingKey)`; all selected IDs are `done`. |
| REAL-OK-09 | Mixed SQS standard and FIFO destinations, all successful. | Standard queues batch by 10; FIFO queues send one message at a time per group; each FIFO group preserves order; independent standard/FIFO destinations may progress concurrently. |
| REAL-OK-10 | Large successful selected batch includes duplicate-looking values across non-ID columns. | Delete accounting uses only selected event IDs, not payload/topic/queue/body values; every selected ID is deleted exactly once. |
| REAL-OK-11 | 250 valid standard SQS events for one queue with `COLLECT_BATCH_TARGET=100`. | Three processor loops drain the backlog as 100, 100, 50 selected events; SQS sends 10, 10, 5 standard batches. |
| REAL-OK-12 | 250 valid standard SQS events split across 5 queues, 50 per queue, with `COLLECT_BATCH_TARGET=250`. | One processor loop can select all 250 valid events; each queue receives 5 batches of 10. |
| REAL-OK-13 | 120 old events for a broken SQS queue and 20 newer events for a healthy Pub/Sub topic, with `COLLECT_BATCH_TARGET=40`. | Computes an effective cap of 20 per route, selects 20 broken SQS events and all 20 Pub/Sub events; Pub/Sub successes are committed, SQS failures remain. |
| REAL-OK-14 | Table contains 20 unknown-target events older than 20 valid SQS events. | Selects and sends the 20 valid SQS events; unknown-target rows remain pending and unlogged by the processor. |

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
| PS-OK-06 | Many events for one ordering key are selected. | All selected events are attempted sequentially; collection, timeout, and watchdog progress bound the run. |
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
| PS-PRE-11 | Empty data, no Pub/Sub attributes, no `options.pubsub.orderingKey`. | P4; no publish call. |
| PS-PRE-12 | Empty data, no Pub/Sub attributes, `options.pubsub.orderingKey` present. | Not local poison; publish in isolation before classification. |
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
| PS-FAIL-13 | Many topics are active while `SQS_SEND_CONCURRENCY=1`. | Pub/Sub still uses publisher flow control and per-key sequencing; `SQS_SEND_CONCURRENCY` does not serialize Pub/Sub publishes. |

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
| SQS-OK-07 | Many FIFO events for one message group are selected. | All selected events are attempted sequentially as single-message sends. |
| SQS-OK-08 | FIFO event has no `options.sqs.messageGroupId`. | Stable provider-safe synthetic `MessageGroupId` is derived from event ID. |
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
| SQS-PRE-07 | More than 10 native SQS message attributes. | P5; no provider call. |
| SQS-PRE-08 | Attribute name is invalid: empty, starts with `AWS.`/`Amazon.`, starts/ends with `.`, has `..`, or contains invalid chars. | P5; no provider call. |
| SQS-PRE-09 | Attribute name or type exceeds 256 chars. | P5; no provider call. |
| SQS-PRE-10 | Attribute value contains unsupported Unicode. | P4 or P5 as implemented, but must be local poison and not provider retry. |
| SQS-PRE-11 | FIFO `options.sqs.messageGroupId` exceeds 128 chars. | P6; no provider call. |
| SQS-PRE-12 | FIFO `options.sqs.messageGroupId` contains invalid characters. | P6; no provider call. |
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
| ORDER-01 | Batch 1 selects ordered events 1-8 for a key; batch 2 selects 9-16. | Provider sees 1-16 in order across batches if all succeed. |
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
routing failures are never deleted as poison. Also cover collection early:
routing failures are left unselected until they become routable.
