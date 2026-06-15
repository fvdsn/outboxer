# Event processing — requirements

Status: **requirements agreed.** The design follows once these are stable, and
will be delivered in stages (see end). Nothing here is implemented yet.

Processing has two steps: **collecting** the events to send, and **sending**
them. Collection stays as-is; sending is redesigned.

## Step 1 — Collecting events (committed: keep as-is)

Requirements:

- **Fair across queues** — a hot queue must not starve the others.
- **Safe under multiple instances** — especially for ordering. Processing events
  from the same ordered queue on multiple instances concurrently would break
  ordering.
- **Stable, deterministic Postgres connection count.**

The current collector already satisfies these, so we keep it:

- It collects the top N events across all queues, first-come-first-served by `id`
  (an old, starved queue's events bubble to the front — fair by construction).
- It uses a single Postgres connection.
- With `FOR UPDATE` (no `SKIP LOCKED`), a second instance blocks rather than
  processing in parallel, so only one instance is ever actively collecting —
  safe for ordering, at the cost of being hot-standby rather than scale-out.

## Step 2 — Sending events (to be redesigned)

### Correctness invariant

- **S0 — Never lose an event (at-least-once).** An event is deleted only after
  the queue confirms receipt, never before. Duplicates are acceptable (consumers
  must be idempotent); loss is not. Every other requirement is subordinate to
  this one.

### Requirements

- **S1 — Low fixed per-batch overhead.** No large setup or artificial
  accumulation delay; sending starts promptly. (Note: the collection step has
  already accumulated the batch, so the sender can flush immediately and still
  form full provider batches — batching and low latency do not conflict here.)
- **S2 — Ordered queues keep their order**, both within a batch and across
  consecutive batches (a capped queue's remainder continues in `id` order next
  time).
- **S3 — Parallelized as much as possible, with deterministic, stable bounds** —
  total send concurrency is a configured number, not something that multiplies
  with batch composition or queue count.
- **S4 — No single queue may dominate a batch.** An ordered queue can't be
  parallelized, so a long run of its events must not extend a batch's send time
  unboundedly or deny other queues their share while they fill up. (Today's
  max-sequential limit is one candidate mechanism, not the requirement itself.)
- **S5 — Recover from transient sender failures, including credential
  rotation.** Mechanism: cache senders and rely on the SDKs' automatic
  credential refresh; recreate a sender only on persistent failure. (Explicitly
  *not* recreating per batch — that fights S1, and the Go AWS/Google clients
  refresh credentials on their own.)
- **S6 — Delete confirmed-sent events as soon as possible**, so an interrupted
  batch re-sends as few already-sent events as possible. (Bounded by the commit
  model — see staging.)
- **S7 — One clean sender interface** satisfied by both SQS and Pub/Sub.
- **S8 — Per-event / per-group result reporting.** The sender reports which
  events were confirmed, so we delete exactly those and retry the rest (handles
  partial-batch failure; the spine of S0 and S6).
- **S9 — Bounded, non-hanging sends.** A stuck send times out
  (`PUBLISH_TIMEOUT_MS`); the processor never blocks forever on one queue
  (liveness, distinct from S4's fairness).
- **S10 — Interruptible.** Sending respects context cancellation for graceful
  shutdown; at-least-once (S0) covers any in-flight events that were interrupted.
- **S11 — Room for poison-event handling.** A permanently un-sendable event must
  not wedge processing forever. Asymmetry to respect: for **unordered** queues a
  poison event can be dropped / dead-lettered and skipped; for **ordered** queues
  it cannot be skipped without breaking order, so it necessarily blocks that
  queue until removed. The design must leave room for this (future dead-letter
  mechanism).

## Staging

The processor will be built in stages so correctness lands before optimization:

- **Stage 1 — correctness.** Single commit at the end of each batch. Re-delivery
  on interrupt is coarse (a whole batch), but the model is simplest and clearly
  correct. Satisfies S0–S5, S7–S11.
- **Stage 2 — eager deletion.** Incremental commits: delete and commit each
  confirmed sub-group as it lands, to satisfy S6 fully (minimal re-delivery on
  interrupt). Builds on Stage 1 without changing the sender interface.
