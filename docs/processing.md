# Event processing — requirements & Stage 1 design

Status: **requirements agreed; Stage 1 design proposed.** Not implemented yet.

Processing has two steps: **collecting** the events to send, and **sending**
them. Collection stays as-is; sending is redesigned and delivered in stages.

## Step 1 — Collecting events (committed: keep as-is)

Requirements:

- **Fair across queues** — a hot queue must not starve the others.
- **Safe under multiple instances** — especially for ordering. Processing events
  from the same ordered queue on multiple instances concurrently would break
  ordering.
- **Stable, deterministic Postgres connection count.**

The current collector already satisfies these, so we keep it:

- Top N events across all queues, first-come-first-served by `id` (an old,
  starved queue's events bubble to the front — fair by construction).
- A single Postgres connection.
- `FOR UPDATE` (no `SKIP LOCKED`) → a second instance blocks rather than
  processing in parallel, so only one instance is ever actively collecting
  (hot-standby, ordering-safe).

## Step 2 — Sending events (to be redesigned)

### Correctness invariant

- **S0 — Never lose an event (at-least-once).** Delete only after the queue
  confirms receipt, never before. Duplicates are acceptable (idempotent
  consumers); loss is not. Everything else is subordinate to this.

### Requirements

- **S1 — Low fixed per-batch overhead.** No large setup or artificial
  accumulation delay. (The collection step already accumulated the batch, so the
  sender can flush immediately and still form full provider batches.)
- **S2 — Ordered queues keep their order**, within a batch and across consecutive
  batches.
- **S3 — Parallelized as much as possible, with deterministic, stable bounds** —
  total send concurrency is a configured number, not something that multiplies
  with batch composition or queue count. (Strict for SQS via a global semaphore;
  for Pub/Sub the bound is the client's own flow control.)
- **S4 — No single queue may dominate a batch.** An ordered queue can't be
  parallelized, so a long run of its events must not extend a batch's send time
  unboundedly or deny other queues their share. (Max-sequential cap is one
  candidate mechanism.)
- **S5 — Recover from transient sender failures, including credential rotation.**
  Cache senders and rely on the SDKs' automatic credential refresh; recreate a
  sender only on persistent failure. (Explicitly not per-batch recreation.)
- **S6 — Delete confirmed-sent events as soon as possible** (bounded by the
  commit model — see staging).
- **S7 — One clean sender interface** for both SQS and Pub/Sub.
- **S8 — Per-event / per-group result reporting** — the sender reports which
  events were confirmed, so we delete exactly those and retry the rest.
- **S9 — Bounded, non-hanging sends** (`PUBLISH_TIMEOUT_MS`); never block forever
  on one queue.
- **S10 — Interruptible** — sending respects context cancellation; S0 covers
  interrupted in-flight events.
- **S11 — Poison events are removed.** An event that can never be sent (poison,
  see taxonomy) is removed from the main flow — dropped + logged now,
  dead-lettered once a DLQ exists — so it cannot clog collection or block an
  ordered queue. All other failures are retried (cadence per S12).
- **S12 — Back off, don't busy-loop.** After a batch that made no progress
  (failure or empty table), wait before retrying, so a failing backend isn't
  hammered and an idle table doesn't spin (`ERROR_COOLDOWN_MS`,
  `POLL_INTERVAL_MS`).

## Failure taxonomy

**Poison = can never be sent; retrying produces the identical permanent
failure.** Poison must be removed (S11). Everything else is retried (S12).

### Poison (remove)

Routing (never reaches a backend):

- **P1 — target cannot be routed in principle**: an unknown/unsupported target
  value, or an empty target with *both* backends enabled (ambiguous). (A target
  naming a *disabled* backend is **not** poison — see R-operator-action.)
- **P2 — destination resolves to empty**: null destination and no default.

Backend permanent-reject (deterministic client error; identical retry fails
identically — Pub/Sub `InvalidArgument`, SQS `SenderFault=true`):

- **P3 — payload too large** (Pub/Sub > 10 MB; SQS ≥ 256 KB).
- **P4 — empty/invalid payload** (empty body is rejected).
- **P5 — malformed or over-limit attributes.**
- **P6 — invalid ordering key / message group** (length limits).
- **P7 — syntactically invalid destination** (bad topic name / queue URL format;
  distinct from "not found").

### Retryable (keep, retry with backoff), by resolution condition

Self-healing — resolves on its own / with backoff:

- **R1 — service unavailable / internal / 5xx** → backend recovers.
- **R2 — timeout / deadline** (incl. `PUBLISH_TIMEOUT`) → network/backend
  recovers.
- **R3 — throttling / quota exceeded** → rate window resets.

Operator/infra action — won't self-heal, but not "never":

- **R4 — destination not found** (topic/queue doesn't exist) → operator creates
  it.
- **R5 — permission denied / access denied** → operator grants IAM.
- **R6 — auth / expired credentials** → usually self-heals via SDK refresh (S5);
  else operator fixes credentials.
- **R7 — target names a disabled backend** → operator enables that backend.

Interruption — not really failures:

- **R8 — context canceled** (shutdown) → next run.
- **R9 — ordering key paused** (Pub/Sub `FailedPrecondition` after a prior
  failure) → `ResumePublish` + the prior cause resolving.

### Residual risk

Poison removal fixes the *never-drainable* clog. Operator-action failures
(R4–R7) are **not** poison (they could succeed later), so we cannot drop them
without risking loss — but with the global `ORDER BY id` collection, a
badly-misconfigured destination's events accumulate at the front and can crowd
the window until the operator fixes it. This is an accepted limitation of the
committed collection model; surface it via metrics/alerting ("events failing for
destination X for N minutes") rather than per-queue isolation.

### Changes vs. current behavior

- SQS oversized + sender-fault → already dropped. ✓
- Unroutable-in-principle (P1) is currently *left* and accumulates → becomes
  poison and is removed.
- Pub/Sub oversized/invalid is currently retried forever → becomes poison.
- Disabled-backend target → stays retryable (unchanged: left in place).

## Staging

- **Stage 1 — correctness.** A single commit at the end of each batch.
  Re-delivery on interrupt is coarse (a whole batch), but the model is simplest
  and clearly correct. Satisfies S0–S5, S7–S12.
- **Stage 2 — eager deletion.** Incremental commits: delete + commit each
  confirmed sub-group as it lands (S6), without changing the sender interface.

## Stage 1 design

Each element is tagged with the requirement(s) it upholds.

### Sender interface — [S7, S8]

```go
// A sender publishes events for one backend. Grouping, batching, ordering, and
// concurrency bounds are internal to it. [S7]
type sender interface {
    // Send publishes what it can and returns the events that are done and may be
    // deleted (confirmed sent, or removed as poison). Events not returned stay
    // for a later batch. err signals a systemic failure → backoff. [S8]
    Send(ctx context.Context, events []event) (done []event, err error)
    Close() error
}
```

The `done` set is the result reporting [S8]; "delete only `done`" is what makes
[S0] hold. Poison events are included in `done` (removed); retryable failures are
omitted (kept). [S11]

### Batch orchestration — single commit

1. `BEGIN` on the one connection — [collection: stable connections]
2. `SELECT … ORDER BY id LIMIT N FOR UPDATE` — [collection: fairness, ordering]
3. Route to `pubsub` / `sqs` / poison; route-poison (P1, P2) → removed — [S11, S0]
4. `pubsubSender.Send` and `sqsSender.Send` **concurrently**, under a
   `PUBLISH_TIMEOUT` context that is also the shutdown context — [S3, S9, S10]
5. `done = pubsubDone ∪ sqsDone ∪ routePoison`
6. `DELETE … WHERE id IN (done)` — [S0]
7. `COMMIT` — [Stage 1 single commit; coarse half of S6]
8. If err, or events were selected but `done` is empty, back off — [S12]

### Pub/Sub sender

- One cached `Publisher` per topic, `EnableMessageOrdering=true`, `Stop` on
  `Close`, recreate on persistent failure — [S5, S1]
- Cap events per ordering key — [S4]
- Fire all `Publish()` then `Get()` all — [S1, S3]
- Key failure → `ResumePublish(key)` and stop that key's remainder (kept) — [S2,
  S8]; identify poison (P3–P7) and include in `done` — [S11]

### SQS sender

- Cached `sqs.Client`, recreate on persistent failure — [S5, S1]
- Group by queue, detect FIFO by `.fifo`, cap per message group — [S4]; chunk 10
- FIFO: same group sequential, other groups concurrent; standard: concurrent —
  [S2, S3]
- One global `SQS_SEND_CONCURRENCY` semaphore across all SQS sends — [S3]
- Sender-fault / oversized (P3–P7) → removed (`done`); transient → kept — [S11,
  S0]

### Coverage

| Req | Upheld by |
| --- | --- |
| S0 | delete only `done`, after confirmation |
| S1 | cached senders; fire-then-get, no accumulation delay |
| S2 | Pub/Sub keys + ResumePublish; SQS FIFO groups; cross-batch id order |
| S3 | concurrent backends; SQS global semaphore; Pub/Sub client flow control |
| S4 | per-ordering-group cap |
| S5 | cached + SDK auto-refresh + recreate-on-persistent-failure |
| S6 | partial (batch-end commit); full in Stage 2 |
| S7 | the `sender` interface |
| S8 | the `done` set |
| S9 | `PUBLISH_TIMEOUT` per Send |
| S10 | shutdown ctx through Send; `Close` |
| S11 | poison (P1–P7) → `done`/removed; retryable kept |
| S12 | step 8 backoff |

### Config implied

- Remove: `BATCH_WORKERS`, `BATCH_MAX_SEQUENTIAL`.
- Keep: `BATCH_SIZE`, `ERROR_COOLDOWN_MS`, `POLL_INTERVAL_MS`, `PUBLISH_TIMEOUT_MS`.
- Add: `SQS_SEND_CONCURRENCY`; a per-ordered-group cap (replacing
  `BATCH_MAX_SEQUENTIAL`'s intent).
