# Event processing requirements & Stage 1 design

Status: **requirements agreed; Stage 1 implementation in progress.**

Processing has two steps: **collecting** the events to send, and **sending**
them. Collection supports two modes; sending is redesigned and delivered in
stages.

## Step 1 — Collecting events

### Shared collection requirements

- **Safe under multiple instances** — especially for ordering. Processing events
  from the same ordered queue on multiple instances concurrently would break
  ordering.
- **Stable, deterministic Postgres connection count.** Collection and deletion
  happen on a single connection in one database transaction.
- **One active collector without blocking producers.** The processor locks the
  selected rows with `FOR UPDATE` inside the batch transaction, matching the JS
  implementation. Do not use `SKIP LOCKED`. A second Outboxer instance blocks on
  the same oldest selected rows rather than processing in parallel, while normal
  producers can still insert new rows into the table.
- **One commit point.** In Stage 1, selected events are sent, `done` events are
  deleted, and the transaction commits once at the end of the batch.
- **Optional columns and default destinations still work.** Any collector mode
  must support the same schema flexibility as routing:
  - the target column may be absent when exactly one backend is enabled;
  - the destination column may be absent when the enabled backend(s) have default
    destinations configured;
  - missing optional target/destination columns participate in grouping through
    their resolved routing/default values, not by requiring new database columns.

### Collection mode A — `global_ordered`

This is the current collection behavior.

- Select at most `COLLECT_GLOBAL_LIMIT` events across the whole table, ordered
  by event id.
- This provides first-come-first-served progress by age across all events.
- It does not guarantee equal per-destination share. A large old backlog or a
  broken old destination can fill consecutive batches until it is fixed or the
  backlog drains.

Shape:

```sql
SELECT *
FROM events
ORDER BY id
LIMIT COLLECT_GLOBAL_LIMIT
FOR UPDATE;
```

The real query uses configured table/column names and runs inside the batch
transaction.

### Collection mode B — `per_route_ordered`

This mode is intended for deployments where one broken destination must not
completely stop unrelated destinations.

Requirements:

- This is the default mode.
- Select up to `COLLECT_PER_ROUTE_LIMIT` events per eligible resolved
  `(target, destination)` route, ordered by event id within each route.
- Consider **all** eligible resolved `(target, destination)` routes in the table
  in the same collection query, not only a subset of routes chosen by a prior
  query.
- Do **not** apply `COLLECT_GLOBAL_LIMIT` in this mode. A global cap is
  fundamentally incompatible with the requirement to select across all routes:
  it would reintroduce a subset of routes chosen by global age.
- Only events that resolve to an enabled backend and a concrete destination are
  eligible for collection in this mode. Routing failures (R7, R10-R12) are not
  selected, logged, or sent by `per_route_ordered`; they remain in the table
  until configuration or code changes make them routable, or until an operator
  intentionally uses `global_ordered`.
- A valid route whose provider sends are broken or retrying can consume at most
  `COLLECT_PER_ROUTE_LIMIT` slots in one selected batch and therefore cannot
  occupy the entire selected batch while other valid routes have eligible
  events.
- This mode does not make healthy routes independent of broken routes within the
  same Stage 1 transaction. A broken route can still slow the batch until its
  bounded sender operations return, because all `done` events commit together at
  the end. Early break / circuit behavior for broken routes is a later
  improvement.
- The selected batch size is data-dependent:
  `selected <= distinct_resolved_routes × COLLECT_PER_ROUTE_LIMIT`. This is an
  intentional tradeoff. It preserves route isolation, but transaction time and
  send time scale with the number of routes that have pending events.
- This is destination fairness, not retry scheduling. If every selected route is
  broken, the processor may still make no deletion progress; sender failures
  still follow S12.

The grouping key is the **eligible resolved route**:

- `resolved_target` is the event target when the target column exists and is
  non-empty; when the target column is absent or empty and exactly one backend is
  enabled, it is that enabled backend. Any other target state is a retryable
  routing failure and is not eligible in this mode.
- `resolved_destination` is the event destination when the destination column
  exists and is non-empty; otherwise it is the routed backend's configured
  default destination when one exists. An empty resolved destination is a
  retryable routing failure and is not eligible in this mode.
- Known-but-disabled targets, unknown targets, ambiguous empty targets, and
  missing destinations still remain retryable routing failures. This mode simply
  does not select them.

Shape:

```sql
WITH routable AS (
  SELECT
    id,
    resolved_target,
    resolved_destination,
    row_number() OVER (
      PARTITION BY resolved_target, resolved_destination
      ORDER BY id
    ) AS route_rank
  FROM events
  WHERE is_routable
),
ranked AS (
  SELECT id
  FROM routable
  WHERE route_rank <= COLLECT_PER_ROUTE_LIMIT
)
SELECT events.*
FROM events
JOIN ranked USING (id)
ORDER BY events.id
FOR UPDATE;
```

The real implementation must generate `resolved_target`, `resolved_destination`,
and `is_routable` from the configured columns, enabled backends, and backend
defaults. `is_routable` is true only when the event resolves to an enabled
backend and a non-empty destination. If a column is not configured or not present
because it is optional, the generated SQL must use the corresponding default or
routing expression instead of referencing the missing column. The sketch uses
`id` for readability; the real query uses the configured event id column for
ranking and joining. The ranking query may compute synthetic columns, but the
final projection must return only base event-table columns so synthetic values do
not leak into `event.columns` or collide with user columns.

## Step 2 — Sending events (to be redesigned)

### Correctness invariant

- **S0 — Never lose a sendable event (at-least-once).** Delete sendable events
  only after the queue confirms receipt, never before. Duplicates are acceptable
  (idempotent consumers); loss is not. Poison events are the explicit exception:
  they are removed only after being classified as unsendable (S11). Everything
  else is subordinate to this.

### Requirements

- **S1 — Low fixed per-batch overhead.** No large setup or artificial
  accumulation delay. The collection step already accumulated the batch, so
  senders flush immediately when the provider client buffers internally. Provider
  batch calls are used only where they do not weaken ordering or result
  semantics.
- **S2 — Ordered queues keep their order**, within a batch and across consecutive
  batches. For ordered Pub/Sub keys and SQS FIFO message groups, never have a
  later event in flight before the earlier event's outcome is known.
- **S3 — Parallelized as much as possible, with deterministic, stable bounds.**
  SQS send concurrency is a configured global semaphore, not something that
  multiplies with batch composition or queue count. Pub/Sub concurrency is
  bounded by the publisher client's flow control and by per-ordering-key
  sequencing; it is not controlled by `SQS_SEND_CONCURRENCY`.
- **S4 — Bound ordered-group work per batch.** An ordered group can't be
  parallelized, so a long run of events for one Pub/Sub ordering key or SQS FIFO
  message group must not extend a batch's send time unboundedly. This is a
  send-time bound, not per-queue collection fairness.
- **S5 — Recover from transient sender failures, including credential rotation.**
  Cache senders for the process lifetime and rely on the SDKs' automatic retry
  and credential refresh. Do not recreate clients/publishers in-process; if a
  client reaches unrecoverable internal state, commit `done`, stop cleanly, and
  let process restart create fresh clients.
- **S6 — Delete confirmed-sent events as soon as possible** (bounded by the
  commit model — see staging).
- **S7 — One clean sender interface** for both SQS and Pub/Sub.
- **S8 — Per-event / per-group result reporting** — the sender reports which
  events were confirmed, so we delete exactly those and retry the rest. When a
  provider reports a failure only at group/bundle granularity, the sender must
  either prove poison locally before sending or isolate the failure to a single
  event before removing any event as poison.
- **S9 — Bounded, non-hanging publish operations** (`PUBLISH_TIMEOUT_MS`); never
  block forever on one queue. The timeout applies to each provider publish
  operation, not necessarily the whole backend sender call. For provider clients
  with internal async work, the timeout must bound that internal publish work,
  not only the caller waiting for a result. A caller-side wait timeout is only
  allowed after the provider's own publish timeout should already have resolved
  the send. `PUBLISH_TIMEOUT_MS` must be positive for every enabled backend.
  Disabling it would leave SQS sends bounded only by SDK/transport defaults or
  the process watchdog, and would make ordered Pub/Sub outcomes unbounded. The
  worst-case batch send time must be derived from the enabled backend limits.
  Since Pub/Sub and SQS send concurrently, the batch send bound is the maximum of
  the enabled backend bounds, not their sum. In `global_ordered` mode the
  selected-event bound is `COLLECT_GLOBAL_LIMIT`. In `per_route_ordered` mode
  the selected-event count is data-dependent because every route contributes up
  to `COLLECT_PER_ROUTE_LIMIT`; the implementation must compute send bounds from
  the actual selected batch composition before sending, or otherwise keep the
  watchdog alive during long valid batches.
  - `pubsubBound = ORDERED_GROUP_BATCH_CAP × (PUBLISH_TIMEOUT_MS +
    PUBLISH_RESULT_GRACE_MS)` (one slow ordered key sending its capped run
    sequentially).
  - `sqsStandardBound = ceil(selected_standard_sqs_events / SQS_SEND_CONCURRENCY) ×
    PUBLISH_TIMEOUT_MS` (conservative bound for standard queue batch requests in
    semaphore-limited waves; count chunks are 10 messages, but byte-size splits
    can force one-message requests).
  - `sqsFifoBound = max(ceil(selected_fifo_sqs_events / SQS_SEND_CONCURRENCY),
    selected_max_fifo_group_events) × PUBLISH_TIMEOUT_MS`, where
    `selected_max_fifo_group_events <= ORDERED_GROUP_BATCH_CAP` (covers both
    many independent FIFO groups limited by the global semaphore and one hot FIFO
    group sending its capped run sequentially).
  - `batchSendBound = max(enabled(pubsubBound), enabled(sqsStandardBound),
    enabled(sqsFifoBound))`.
  The watchdog heartbeat must advance during long-running batches, not only once
  per batch. The processor/senders must advance it after meaningful progress:
  selection completed, each bounded provider operation/result completed,
  database delete completed, and commit completed. Static startup validation of a
  full batch bound is sufficient only for `global_ordered`, where the
  selected-event bound is configured. In `per_route_ordered`, the selected route
  count is data-dependent, so a legitimately large per-route batch must not trip
  a false deadlock as long as its individual bounded operations keep making
  progress.
- **S10 — Interruptible** — sending respects context cancellation; S0 covers
  interrupted in-flight events.
- **S11 — Poison events are removed.** Poison means the selected backend can
  never accept the event content as-is (P3–P7). These events are dropped + logged
  now (status quo for SQS oversized/sender-fault), and dead-lettered once a DLQ
  exists. Routing failures are not poison: an unknown target might become
  supported after an Outboxer upgrade, an ambiguous target might become routable
  after configuration changes, and a missing destination can be fixed by adding a
  default. Routing failures are kept. In `global_ordered` mode, selected routing
  failures are logged through the bounded failure logger and can clog collection
  until fixed. In `per_route_ordered` mode, routing failures are not eligible for
  selection, so they are neither sent nor logged by the processor. All other
  failures are retried (cadence per S12).
- **S12 — Back off on database errors only.** Busy-looping the DB is desired (it
  keeps latency low and is not harmful), so it is the default. Only a
  *database/transaction* error (BeginTx/SELECT/DELETE/COMMIT failing — connection
  loss, timeout, too-many-connections) triggers a cooldown (`ERROR_COOLDOWN_MS`),
  to avoid hammering an overloaded database. Send failures and empty batches do
  **not** trigger backoff:
  - Sender overload is handled by the SDK clients' own retry/backoff (AWS:
    standard retryer, 3 attempts, exponential backoff + jitter; Pub/Sub: gax
    retry + flow control). A throttled send just makes `Send` take longer
    (bounded by `PUBLISH_TIMEOUT_MS`), which paces the loop naturally.
  - An empty table keeps polling immediately unless `POLL_INTERVAL_MS` is set.
- **S13 — Bound failure-log volume.** A retryable event stuck in the retry loop
  must not emit a log per attempt. Failure logs are rate-limited / aggregated per
  failure signature (e.g. first occurrence immediately, then a periodic summary
  with a suppressed count), so a single recurring failure produces a bounded log
  rate regardless of how fast the loop runs.

## Provider client behavior constraints

The sending design is constrained by the queue clients' actual acknowledgement
and batching semantics.

### Google Pub/Sub Go publisher

- `Publisher.Publish` is asynchronous: it returns a `PublishResult`, and
  `Get(ctx)` waits for that result. The publisher owns background batching and
  send goroutines; `Stop` flushes remaining messages and stops those goroutines.
  ([Go package docs](https://pkg.go.dev/cloud.google.com/go/pubsub/v2#Publisher))
- Pub/Sub has an internal batching delay. Outboxer must call `Flush()` after
  enqueueing the intended publish set and before waiting on `Get()` results, so
  batch latency is controlled by Outboxer rather than the client's delay
  threshold. Tuning `DelayThreshold` is an optimization, not a substitute for the
  explicit flush.
- The caller's `Get(ctx)` context bounds how long Outboxer waits for a result,
  but the publisher also has its own `PublishSettings.Timeout` for publish RPCs.
  `PUBLISH_TIMEOUT_MS` configures the publisher timeout for each publish RPC. The
  `Get` wait must be longer than that provider timeout
  (`PUBLISH_TIMEOUT_MS + PUBLISH_RESULT_GRACE_MS`), otherwise a timed-out `Get`
  could leave an in-flight publish that succeeds later. If `Get` still times out
  after the grace period, Outboxer treats the publisher/key as having an unknown
  in-flight send: it must not publish later ordered events for that key in this
  process.
- For Pub/Sub ordering keys, a sender must keep at most one event in flight per
  key. It publishes one event, flushes, waits for its result, and only then
  publishes that key's next event. Different ordering keys may run concurrently,
  and unordered events may still use normal async batching.
- If an ordered Pub/Sub send becomes unknown despite the provider timeout and
  grace wait, the safe recovery is to stop processing and let the process restart
  after `Stop`/shutdown handling. Continuing with the same key risks a late
  success for the earlier event appearing after a later event.
- If an unordered Pub/Sub send becomes unknown after the provider timeout and
  grace wait, omit that event from `done` and continue. It may later be accepted
  and then retried, producing a duplicate, which is allowed by S0 because there is
  no ordering contract for that event.
- Pub/Sub publishers are process-lifetime objects. Do not recreate a publisher
  while processing; otherwise two publishers can race on the same ordering key if
  the old publisher still has outstanding ordered messages. Fresh publishers are
  created only on process start.
- The publisher batches messages internally. A backend publish RPC failure can
  fail every `PublishResult` in a bundle with the same error, so Pub/Sub may not
  identify which event in the bundle caused a permanent `InvalidArgument`.
  Therefore Pub/Sub poison handling cannot rely on bundle-level permanent errors
  as per-event proof.
- Pub/Sub poison handling is two-phase: first, local prevalidation removes known
  transport-level poison before publish (size, attribute count/length, definitely
  invalid empty message, syntactically invalid topic). Second, if a normal
  bundled publish returns an ambiguous permanent backend reject, the sender
  retries that failed bundle in isolation mode: publish one event, flush/get its
  result, classify only that event, then continue. Only an isolated permanent
  failure is enough to mark an event `done` as poison.
- For ordering keys, a publish failure pauses the key. The sender must call
  `ResumePublish(key)` before future sends for that key, and must keep the
  failed event plus that key's later events out of `done`.

### Amazon SQS sender

- `SendMessageBatch` accepts up to 10 messages and reports each entry
  individually. A request can return HTTP 200 with a mix of `Successful` and
  `Failed` entries, so callers must inspect both lists.
  ([API docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html))
- For FIFO queues, SQS enqueues the entries it accepts in the order they were
  sent. That is not the same as all-or-nothing acceptance: if entries 1 and 3
  succeed while entry 2 has a retryable failure, SQS has preserved the order of
  accepted messages (`1, 3`) but the outbox order (`1, 2, 3`) now has a hole.
- Because of that partial-acceptance behavior, Outboxer must not batch-send
  multiple events from the same FIFO message group. FIFO sends are single-message
  operations per group; different groups may still be sent concurrently under the
  global SQS semaphore. Standard queues can still use `SendMessageBatch` chunks
  of 10.

## Local prevalidation criteria

Local prevalidation exists to keep known poison out of provider batch calls and
to make Pub/Sub bundle errors less ambiguous. It must be conservative: mark an
event poison locally only when a documented provider rule proves the event cannot
be sent as-is. If the rule is ambiguous, implementation-dependent, or affected by
destination configuration, publish normally and use single-event isolation before
removing the event as poison.

Validation runs after Outboxer's existing string-attribute sanitization. Current
compatibility behavior is kept: non-string attributes are dropped and logged
rather than making the event poison. Once attributes have been sanitized to
strings, documented provider attribute limits apply.

### Pub/Sub

Use the current documented Pub/Sub limits
([quotas](https://docs.cloud.google.com/pubsub/quotas),
[message schema](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage),
[publisher attributes](https://docs.cloud.google.com/pubsub/docs/publisher),
[resource names](https://docs.cloud.google.com/pubsub/docs/pubsub-basics#resource_names)):

- `pubsubMaxMessageDataBytes = 10_000_000` (10 MB data field).
- `pubsubMaxPublishRequestBytes = 10_000_000` (10 MB total publish request).
- `pubsubMaxPublishRequestMessages = 1000`.
- `pubsubMaxAttributes = 100`.
- `pubsubMaxAttributeKeyBytes = 256`.
- `pubsubMaxAttributeValueBytes = 1024`.

Provider docs use decimal byte units (`1 kB = 1000 bytes`), so Pub/Sub constants
use decimal MB, not MiB.

Classify as local Pub/Sub poison:

- P3 if the event data alone exceeds `pubsubMaxMessageDataBytes`.
- P3 if a single event cannot fit into a publish request after accounting for
  its serialized message overhead. If an implementation cannot compute exact
  protobuf request size, it may use a conservative estimate to split earlier,
  but must not delete on an estimated overflow unless a single isolated publish
  confirms the permanent size failure.
- P4 if data is empty and there are no attributes and no ordering key. Provider
  docs are inconsistent about whether an ordering key alone is acceptable, so an
  ordering-key-only Pub/Sub message is not local poison; publish it in isolation
  before classifying.
- P5 if sanitized string attributes exceed the documented count/key/value limits
  or if an attribute key starts with `goog`.
- P7 if the destination is syntactically impossible as a Pub/Sub topic. Accept
  either a full `projects/{project}/topics/{topic}` name or a bare topic ID that
  the Go client resolves in its configured project. The topic ID must start with
  a letter, contain 3–255 characters, not start with `goog`, and contain only
  letters, digits, `-`, `_`, `.`, `~`, `+`, or `%`.

Do not classify these locally as poison:

- A multi-event publish request exceeding 10 MB or 1000 messages. Split the
  publish set; only a single-event overflow can be P3.
- Topic not found, permission denied, auth failure, quota exhaustion, or schema
  validation failures observed only at bundle level. Use retryable classification
  or single-event isolation as appropriate.
- Pub/Sub ordering-key length or character content unless a documented provider
  limit is encoded. A permanent backend reject for an isolated event may still
  become P6.

### SQS

Use the current documented SQS limits
([message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html),
[SendMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html),
[SendMessageBatchRequestEntry](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatchRequestEntry.html),
[message metadata](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html)):

- `sqsMaxMessageBytes = 1_048_576` (1 MiB body + message attributes).
- `sqsMaxBatchEntries = 10`.
- `sqsMaxBatchBytes = 1_048_576` (sum of batched message bodies and
  attributes).
- `sqsMaxAttributes = 10`.
- `sqsMaxAttributeNameChars = 256`.
- `sqsMaxAttributeTypeChars = 256`.
- `sqsMaxFifoMessageGroupIDChars = 128`.
- `sqsMaxFifoMessageDeduplicationIDChars = 128`.
- `sqsMaxBatchEntryIDChars = 80`.

Classify as local SQS poison:

- P3 if one message body plus attributes exceeds `sqsMaxMessageBytes`.
- P4 if the body is empty, or if the body/string attribute values contain
  characters outside SQS's allowed Unicode set: tab, line feed, carriage return,
  `#x20`–`#xD7FF`, `#xE000`–`#xFFFD`, or `#x10000`–`#x10FFFF`.
- P5 if sanitized string attributes exceed 10 entries, contain empty/null
  name/type/value components, have an invalid name, or exceed name/type limits.
  Valid names contain only letters, digits, `_`, `-`, and `.`, must not start
  with `AWS.` or `Amazon.` in any casing, must not start/end with `.`, and must
  not contain consecutive periods.
- P6 if a provided FIFO ordering key cannot be used as a `MessageGroupId`
  because it exceeds 128 characters or contains characters outside the SQS FIFO
  ID character set: alphanumeric plus
  ``!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~``.
- P7 if the queue URL is syntactically invalid. A syntactically valid but missing
  queue is R4, not poison.

Do not classify these locally as poison:

- A standard-queue batch exceeding 10 entries or 1 MiB. Split the batch; only an
  individual oversized message can be P3.
- Raw event IDs that are too long or contain characters invalid for
  `MessageDeduplicationId` or batch entry `Id`. Those are Outboxer-generated
  transport identifiers, not event semantics: derive stable provider-safe values
  from the event ID, using the raw ID only when already valid and a
  **collision-resistant** digest otherwise (e.g. a full SHA-256 hex, 64 chars ≤
  128). Collision-resistance is mandatory for `MessageDeduplicationId`: a
  collision means SQS treats two distinct events as duplicates within its
  5-minute dedup window and silently drops one — a S0 violation. (For batch entry
  `Id` a collision is only a within-request error, not loss, but use the same
  derivation.)
- FIFO events with no ordering key. They have no cross-event ordering contract,
  but SQS requires a `MessageGroupId`, so derive a stable provider-safe synthetic
  group from the event ID.

## Failure taxonomy

**Poison = the selected backend can never accept the event content as-is;
retrying produces the identical permanent failure.** Poison must be removed
(S11). Everything else is retried or kept for a future operator/code change
(S12). As a rule, an event is *not poison* if the unchanged event could become
sendable through configuration, infrastructure, credentials, quota, or an
Outboxer upgrade. Unknown targets are therefore not poison: a future Outboxer
version might support that target without requiring the event to be edited.

Poison is **removed, not necessarily lost.** Removal means "out of the main flow"
— dropped + logged today, dead-lettered once a DLQ exists. Recovery after poison
removal (manual repair, producer fix, replay) happens out-of-band from logs or
the DLQ.

### Routing failures (keep)

Routing failures never reach a backend. They are kept, because they may become
sendable without editing the event itself:

- **R10 — target unsupported by the current build**: an unknown target value
  (e.g. `kafka`) might become routable after an Outboxer upgrade.
- **R11 — target ambiguous in the current configuration**: empty target with
  multiple enabled backends. A configuration change or later routing feature may
  make the unchanged event routable.
- **R12 — destination resolves to empty**: null destination and no default, after
  the event has been routed to an enabled backend. Adding a default destination
  can make the unchanged event sendable. Disabled-backend routing (R7) takes
  precedence over destination validation.

Routing classification:

| Target value | Enabled backends | Classification |
| --- | --- | --- |
| `pubsub` | Pub/Sub enabled | route to Pub/Sub |
| `sqs` | SQS enabled | route to SQS |
| `pubsub` | Pub/Sub disabled | R7 retryable |
| `sqs` | SQS disabled | R7 retryable |
| empty | exactly one backend enabled | route to the enabled backend |
| empty | both backends enabled | R11 retryable (ambiguous target) |
| unknown value, e.g. `kafka` | any | R10 retryable (unsupported by current build) |

### Poison (remove)

Backend permanent-reject (deterministic client error; identical retry fails
identically). For SQS this can be a batch `SenderFault=true` or equivalent
single-message client error. For Pub/Sub, a bundle-level `InvalidArgument` is
only a permanent-reject signal after local prevalidation or single-event
isolation identifies the specific event:

- **P3 — payload too large** (Pub/Sub data > 10 MB, a single-event Pub/Sub
  publish request > 10 MB, or SQS single message body+attributes > 1 MiB).
- **P4 — empty/invalid payload** (Pub/Sub message with no data/attributes/key
  after the conservative local check or isolated backend proof; SQS empty body or
  body containing unsupported characters).
- **P5 — malformed or over-limit attributes.**
- **P6 — invalid ordering key / message group** (documented provider limit).
- **P7 — syntactically invalid destination** (bad topic name / queue URL format;
  distinct from "not found").

### Retryable (keep, retry later), by resolution condition

Self-healing — resolves on its own / with backoff:

- **R1 — service unavailable / internal / 5xx** → backend recovers.
- **R2 — timeout / deadline** (incl. `PUBLISH_TIMEOUT`) → network/backend
  recovers.
- **R3 — throttling / quota exceeded** → rate window resets.

Operator/infra action — won't necessarily self-heal, but can succeed later
without deleting/changing the event:

- **R4 — destination not found** (topic/queue doesn't exist) → operator creates
  it.
- **R5 — permission denied / access denied** → operator grants IAM.
- **R6 — auth / expired credentials** → usually self-heals via SDK refresh (S5);
  else operator fixes credentials.
- **R7 — target names a known but disabled backend** (`pubsub` while Pub/Sub is
  disabled, or `sqs` while SQS is disabled) → operator enables that backend.
- **R10 — target unsupported by current build** → upgrade Outboxer to a build
  that supports the target.
- **R11 — target ambiguous in current configuration** → operator changes routing
  configuration or upgrades to a routing feature that can resolve it.
- **R12 — destination missing and no default configured** → operator configures a
  default destination.

Interruption — not really failures:

- **R8 — context canceled** (shutdown) → next run.
- **R9 — ordering key paused** (Pub/Sub `FailedPrecondition` after a prior
  failure) → `ResumePublish` + the prior cause resolving.

### Residual risk

Poison removal fixes the *never-drainable content* clog. Operator/code-action
failures (R4–R7, R10–R12) are **not** poison (they could succeed later), so we
cannot drop them without risking loss. In `global_ordered` mode, a
badly-misconfigured destination or unsupported target can accumulate at the
front and crowd the window until the operator fixes it or Outboxer is upgraded,
and selected routing failures are logged through the bounded failure logger. In
`per_route_ordered` mode, routing failures are ignored by collection and remain
pending until they become routable; they cannot crowd selection, but they also
are not logged by the processor while unroutable. A valid route whose provider
sends are broken can still slow a batch until its bounded sender operations
return; Stage 1 commits all `done` events together. The remaining risks are fast
retry of broken valid routes and large batches when many valid routes have
pending events. Surface these via metrics/alerting ("events failing for
destination X for N minutes", "oldest event age by destination", "selected
routes per batch") rather than by dropping retryable events.

The SDK clients pace *throttling/transient* sender errors with their own
backoff, but a *persistent fast-fail* (auth denied, queue not found — R4/R5)
returns immediately, so the busy loop re-hits that sender's API rapidly. Routing
failures (R7, R10–R12) do not hit a provider API. In `global_ordered`, they can
still fast-loop through collection and bounded logging. In `per_route_ordered`,
they are not selected. Log flood is bounded by S13; API re-hits are left to the
managed service's own throttling. If this proves harmful in practice, add a
small no-progress pace specifically for fast-failing sends or routing failures
— not done now, since it trades away the low-latency busy loop we want.

### Changes vs. current behavior

- SQS oversized + sender-fault → already dropped. ✓
- Routing failures (R10–R12) are currently *left* and accumulate → stay kept in
  Stage 1 (unchanged), because dropping them would lose events recoverable by
  config or a future Outboxer build. In `per_route_ordered`, they are not
  selected until they become routable.
- Pub/Sub oversized/known-invalid (content-poison) is currently retried forever →
  becomes dropped poison; ambiguous backend rejects are isolated before any event
  is removed.
- Disabled-backend target → stays retryable (unchanged: left in place).

## Staging

- **Stage 1 — correctness.** A single commit at the end of each batch.
  Re-delivery on interrupt is coarse (a whole batch), but the model is simplest
  and clearly correct. Satisfies S0–S5, S7–S13.
- **Stage 2 — eager deletion.** Incremental commits: delete + commit each
  confirmed sub-group as it lands (S6), without changing the sender interface.
- **Stage 3 — dead-letter queue.** Move poison (P3–P7) into a DLQ instead of
  dropping it, completing S11 without changing retryable routing-failure
  behavior.

## Stage 1 design

Each element is tagged with the requirement(s) it upholds.

### Sender interface — [S7, S8]

```go
// A sender publishes events for one backend. Grouping, batching, ordering, and
// concurrency bounds are internal to it. [S7]
type sender interface {
    // Send publishes what it can and returns the events that are done and may be
    // deleted (confirmed sent, or removed as poison). Events not returned stay
    // for a later batch. err reports sender-level trouble; it does not trigger
    // ERROR_COOLDOWN (S12), and done remains authoritative. Some sender errors
    // can be classified as fatal-after-commit (for example an unknown ordered
    // Pub/Sub publish outcome): delete/commit done first, then stop processing.
    // [S8, S10]
    Send(ctx context.Context, events []event) (done []event, err error)
    Close() error
}
```

The `done` set is the result reporting [S8]; "delete only `done`" is what makes
[S0] hold. Confirmed-sent events and content-poison events (P3–P7) are included
in `done`; retryable failures and routing failures are omitted (kept). [S11]

Sender errors are classified outside the `done` set:

- **Non-fatal sender error**: log/rate-limit it, keep non-`done` events, and
  continue immediately (no `ERROR_COOLDOWN`).
- **Fatal-after-commit sender error**: attempt to delete/commit `done`, then
  stop processing regardless of whether that preservation attempt succeeds. Use
  this for unknown ordered Pub/Sub publish outcomes and unrecoverable client
  state. Continuing in the same process after this error is unsafe, because it
  may publish later ordered events behind an unknown in-flight send. In Go,
  model this as an inspectable sentinel or wrapper (for example
  `errors.Is(err, errFatalAfterCommit)`), not as a string comparison.

### Batch orchestration — single commit

1. `BEGIN` on the one connection — [collection: stable connections]
2. Select events using the configured collection mode (`global_ordered` or
   `per_route_ordered`) and lock the selected rows with `FOR UPDATE`, without
   `SKIP LOCKED`. This serializes Outboxer processors on the oldest selected
   rows while allowing producers to keep inserting new events. — [collection]
3. Route to `pubsub` / `sqs` / retryable routing failure using the routing
   classification table. In `global_ordered`, selected routing failures (R7,
   R10–R12) are **kept** and logged through the bounded failure logger. In
   `per_route_ordered`, routing failures are not selected by the collector. —
   [S11, S0, S13]
4. `pubsubSender.Send` and `sqsSender.Send` **concurrently** under the shutdown
   context. Senders apply `PUBLISH_TIMEOUT` per provider publish operation and
   configure any internal provider timeout needed to make that bound real. — [S3,
   S9, S10]
5. `done = pubsubDone ∪ sqsDone` (each sender's `done` already includes its own
   content-poison P3–P7; routing failures are not in `done`)
6. `DELETE … WHERE id IN (done)` — [S0]
7. `COMMIT` — [Stage 1 single commit; coarse half of S6]
8. On a database/transaction error without a fatal-after-commit sender error,
   back off (`ERROR_COOLDOWN`). Sender failures and empty batches do not back off
   (the busy loop is desired; senders are paced by their clients). If a sender
   reports a fatal-after-commit condition, attempt to delete/commit `done`, then
   stop processing regardless of delete/commit success so the process cannot
   publish behind an unknown in-flight ordered send. A failed preservation
   attempt may cause duplicates on restart, but continuing would risk reordering.
   — [S2, S10, S12]
9. Failure logging goes through a per-signature rate limiter — [S13]

### Pub/Sub sender

- One cached `Publisher` per topic, `EnableMessageOrdering=true`,
  `PublishSettings.Timeout=PUBLISH_TIMEOUT`, `Stop` on `Close`; no in-process
  recreation — [S5, S1, S9, provider: Pub/Sub outstanding publishes]
- Cap events per ordering key — [S4]
- Unordered events: fire all eligible `Publish()` calls, `Flush()` the publisher
  so the batch does not wait on the client's delay threshold, then `Get()` all
  results with `PUBLISH_TIMEOUT + PUBLISH_RESULT_GRACE` as the wait bound — [S1,
  S3, S9, provider: Pub/Sub async publisher]
- Ordered events: group by ordering key; for each key publish one event,
  `Flush()`, `Get()` with `PUBLISH_TIMEOUT + PUBLISH_RESULT_GRACE` as the wait
  bound, and only then publish that key's next event. Different keys may run
  concurrently; one key is strictly sequential. — [S2, S3, S4, S9]
- Key failure → `ResumePublish(key)` and stop that key's remainder (kept) — [S2,
  S8]
- If an ordered event's result is still unknown after the provider timeout plus
  result-wait grace, return a sender error that causes the process to stop after
  preserving `done`; do not send later events for that key in the same process.
  — [S2, S9, S10]
- If an unordered event's result is still unknown after the provider timeout plus
  result-wait grace, omit it from `done`, log/rate-limit the timeout, and continue.
  A later retry may duplicate a late success, which is allowed for unordered
  events. — [S0, S8, S9]
- Identify known poison (P3–P7) with local prevalidation before publishing:
  payload/request size, attribute count and lengths, definitely invalid empty
  message, and topic syntax. — [S8, S11]
- If a normal bundled publish returns an ambiguous permanent backend reject,
  enter isolation mode for the failed bundle: publish one event, `Flush()`,
  `Get()`, classify that single result, and only then include poison in `done`.
  For an ordering key, isolation proceeds only until the first non-done event;
  that event and later events for the same key are kept. — [S2, S8, S11,
  provider: Pub/Sub bundle errors]

### SQS sender

- Cached process-lifetime `sqs.Client`; rely on SDK retry and credential refresh;
  no in-process recreation — [S5, S1]
- Group by queue, detect FIFO by `.fifo` — [S1]
- Standard queues: chunk 10 with `SendMessageBatch`; chunks may run concurrently
  under the global semaphore — [S1, S3, provider: SQS per-entry results]
- FIFO queues: group by message group, cap per message group, and send
  **single-message** requests sequentially within each group. Different groups
  may run concurrently under the global semaphore. Do not batch multiple messages
  from the same FIFO group, because partial `SendMessageBatch` success can create
  accepted holes in the outbox order. **Stop a group at its first non-`done`
  event** (a failure): later events in that group are not sent this batch,
  mirroring the Pub/Sub key rule, so a retry can't enqueue a later event ahead of
  the failed one. — [S2, S3, S4, provider: SQS partial acceptance]
- FIFO events with no ordering key still need an SQS `MessageGroupId`. They do
  not have an ordering contract with each other, so assign a stable synthetic
  group derived from the event id (not a random value). FIFO
  `MessageDeduplicationId` and `SendMessageBatch` entry `Id` are stable
  provider-safe values derived from the event id, using the raw event id only
  when it already satisfies provider limits. — [S0, S2]
- One global `SQS_SEND_CONCURRENCY` semaphore across all SQS sends — [S3]
- Standard batch sender-faults and single-message permanent client errors
  matching P3–P7 → removed (`done`); transient failures → kept — [S11, S0]

### Coverage

| Req | Upheld by |
| --- | --- |
| S0 | delete sendable events only after confirmation; remove poison only after classification |
| S1 | cached senders; Pub/Sub unordered flush/batch; SQS standard batches; no artificial accumulation delay |
| S2 | Pub/Sub one-in-flight per ordered key + ResumePublish; SQS FIFO single-message per group; cross-batch id order |
| S3 | concurrent backends; SQS global semaphore; Pub/Sub client flow control |
| S4 | per-ordering-group cap |
| S5 | process-lifetime cached senders + SDK retry/credential refresh + restart for unrecoverable client state |
| S6 | partial (batch-end commit); full in Stage 2 |
| S7 | the `sender` interface |
| S8 | the `done` set |
| S9 | per-operation provider publish timeout plus longer result wait where async clients need it |
| S10 | shutdown ctx through Send; `Close` |
| S11 | content-poison (P3–P7) → `done`/dropped until DLQ; routing failures and retryable failures kept |
| S12 | cooldown only on DB error; senders paced by their clients |
| S13 | per-signature failure-log rate limiter |

### Config implied

No backward-compatibility guarantee or migration effort is required before
`1.0`. Renamed or removed config keys are not kept as deprecated aliases unless
this spec says so explicitly.

- Remove: `BATCH_WORKERS`, `BATCH_MAX_SEQUENTIAL`.
- Rename: `BATCH_SIZE` → `COLLECT_GLOBAL_LIMIT`; `BATCH_SIZE` is removed, not
  supported as an alias.
- Keep: `ERROR_COOLDOWN_MS`, `POLL_INTERVAL_MS`, `PUBLISH_TIMEOUT_MS`.
- Add:
  - `COLLECTION_MODE` (default: `per_route_ordered`; valid values:
    `global_ordered`, `per_route_ordered`).
  - `COLLECT_GLOBAL_LIMIT` (default: `100`), the maximum rows selected per batch
    in `global_ordered` mode. It does not apply in `per_route_ordered` mode.
  - `COLLECT_PER_ROUTE_LIMIT` (default: `40`), the maximum rows selected per
    resolved `(target, destination)` route in `per_route_ordered` mode.
  - `SQS_SEND_CONCURRENCY` (default: `8`).
  - `ORDERED_GROUP_BATCH_CAP` (default: `8`), the maximum events sent for one
    Pub/Sub ordering key or SQS FIFO message group from a selected batch.
  - `PUBLISH_RESULT_GRACE_MS` (default: `5000`), the extra wait after the
    provider publish timeout for async client results.

`PUBLISH_TIMEOUT_MS` keeps its current default of `30000`. It must be positive
whenever any backend is enabled. For Pub/Sub, ordered sends need a bounded
provider timeout before the result-wait grace can safely expire. For SQS, a
non-positive value would remove Outboxer's explicit send bound and leave the
watchdog as the only guaranteed escape from a stuck send. See Design choices for
why this stays one knob rather than splitting into `PUBSUB_PUBLISH_TIMEOUT_MS` /
`SQS_PUBLISH_TIMEOUT_MS`.

## Design choices

Decisions where a reasonable alternative was considered; both sides recorded so
the reasoning isn't lost.

### Per-key flush vs. one flush per batch round (Pub/Sub ordered keys)

Ordered keys are published one-in-flight-per-key (publish → `Flush` → `Get` →
next). Each key's goroutine calls `Flush`, which is **publisher-global** (it
flushes every key's buffered messages, not just this key's).

- **Chosen: per-key flush.** Each ordered key advances independently; a slow key
  only delays itself. The cost is redundant global `Flush` calls, but `Flush` on
  an already-drained scheduler is near a no-op (other keys are blocked on their
  own `Get` and have nothing buffered), so the waste is small.
- **Rejected: synchronized rounds (one flush per round).** Process round *r* by
  publishing the *r*-th event of every active key, flush once, then `Get` them
  all. Fewer flushes, but it **couples key latencies**: a single slow key's
  `Get` stalls the whole round, so every other key waits at the pace of the
  slowest — which undercuts the spirit of S4 (no one ordered group dominates).
  The marginal flush savings aren't worth trading away key isolation.

### One `PUBLISH_TIMEOUT_MS` vs. per-backend timeouts

- **Chosen: one knob**, positive for every enabled backend. Pub/Sub needs the
  bound for ordered-result recovery; SQS needs it so publish operations are
  bounded by Outboxer rather than only by SDK/transport defaults or watchdog
  process exit.
- **Rejected (for now): `PUBSUB_PUBLISH_TIMEOUT_MS` / `SQS_PUBLISH_TIMEOUT_MS`.**
  Cleaner per-backend semantics and independent tuning, but two knobs and a more
  complex watchdog bound, for a difference most single-backend deployments never
  need. Easy to add later if real latency profiles diverge.
