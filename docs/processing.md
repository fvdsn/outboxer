# Event processing — redesign proposal

Status: **draft / for iteration.** This describes a planned redesign of the send
pipeline. It is not implemented yet.

## Why redesign

The current pipeline (`processEventBatch` + `parallelizeEvents` + worker
goroutines) tangles ordering, batching, parallelism, and deletion together, and
has several problems:

- **Pub/Sub is not batched.** A fresh `Publisher` is created per event, published,
  `Get()`-ed, and `Stop()`-ed, which defeats the client's bundler (one publish
  RPC per event, plus a ~10ms bundler delay per lone message). SQS does batch
  (10 per `SendMessageBatch`).
- **Global "first X" selection causes head-of-line blocking.** `SELECT … ORDER BY
  id LIMIT X` takes the first X rows across *all* destinations, so one stuck or
  slow queue stalls the whole worker pool.
- **Worker fan-out fights the providers.** Events are hashed to one of
  `BATCH_WORKERS` goroutines for sending parallelism, but the Pub/Sub client
  already parallelizes within a topic. The hashing also causes false
  serialization (unrelated keys colliding on a worker) and `BATCH_MAX_SEQUENTIAL`
  silently drops ordered events from a batch.
- **Ordering is partly accidental** — it relies on same-key events landing on the
  same worker and being published in order there, rather than on each provider's
  native ordering.

## Goals

- Batch using each provider's native mechanism (Pub/Sub bundler; SQS 10-message
  batches).
- Preserve ordering per ordering key / FIFO message group.
- Isolate queues: a slow or failing queue must not block others.
- Parallelize across queues.
- Preserve at-least-once delivery.
- Stay safe with multiple Outboxer instances running concurrently.
- Keep the table contract minimal (`id` + `payload` required).

## Core idea: partition by queue

The unit of work becomes a **queue**, identified by `(backend, resolved
destination)` — e.g. `pubsub:topic-a`, `sqs:https://…/queue-b` — after applying
the target column and defaults.

- **One worker owns a queue at a time.** This makes ordering structural: a single
  worker publishes a queue's events in `id` order, and the provider client
  handles finer ordering (Pub/Sub per ordering key; SQS FIFO per message group).
- **Each queue is selected and drained independently** ("first X per queue"), so a
  stuck queue only blocks its own worker.
- **Parallelism is across queues**, via a bounded worker pool. Within a queue we
  rely on the provider client (Pub/Sub parallelizes across keys inside a topic).

### Why not `FOR UPDATE SKIP LOCKED` on rows

Row-level `SKIP LOCKED` would let two workers grab *different rows of the same
queue* and publish them concurrently, reordering the queue. The exclusivity unit
must be the queue, not the row. Row locks cannot express "one worker per queue."

### Queue exclusivity: advisory locks

Use a Postgres **advisory lock per queue**:

```
pg_try_advisory_xact_lock(hash(backend, destination))
```

- Acquired → this worker owns the queue: claim its rows, publish, delete, commit
  (commit releases the lock).
- Not acquired → another worker or instance owns it; skip to the next queue.

This gives "one worker per queue" within a process *and across instances*, with a
`try`-and-skip semantics so busy queues don't pile up blocked waiters. (Hash
collisions between two destinations are possible but only cause a rare, harmless
false skip; the two-key advisory form can reduce this if needed.)

## Proposed shape

**Dispatcher**
1. Enumerate distinct queues that have pending events
   (`SELECT DISTINCT destination …`, refined per backend/target).
2. Hand each queue not already in progress to a bounded worker pool.

**Worker (per queue)**
1. `pg_try_advisory_xact_lock` the queue; if not acquired, release the queue back
   and move on.
2. `SELECT … WHERE destination = $1 ORDER BY id LIMIT X FOR UPDATE` — the queue's
   next batch, in order.
3. Publish via the backend (see below).
4. Delete the events that were published (or are poison/dropped).
5. Commit (releases the advisory lock); repeat for the same queue until it drains
   or yields, then return to the pool.

**Backends** expose a batch API so the orchestrator does not care how publishing
works:

```go
type publisher interface {
    // PublishBatch publishes what it can and returns the ids of events that are
    // done (published, or poison/dropped) and should be deleted.
    PublishBatch(ctx context.Context, events []event) (doneIDs []any, err error)
}
```

- **Pub/Sub backend** — long-lived per-topic `Publisher` (`EnableMessageOrdering =
  true`), fire all `Publish()` then `Get()` all. The client batches/parallelizes;
  ordering keys are native (empty key stays fully concurrent). On an ordered-key
  failure, call `ResumePublish(key)` so the key is not paused across batches.
- **SQS backend** — chunk into 10s and `SendMessageBatch`. FIFO queues send
  sequentially per message group; standard queues may add bounded intra-queue
  concurrency later if a single queue is hot (the AWS SDK does not parallelize on
  its own).

`parallelizeEvents`, `BATCH_WORKERS`, `BATCH_MAX_SEQUENTIAL`, and `strHash` are
removed.

## Ordering guarantees

- Within a queue: one worker, events sent in `id` order.
- Pub/Sub: per ordering key, enforced by the client (sequential per key, parallel
  across keys).
- SQS FIFO: per message group, enforced by sending a group's batches in order.
- Across instances: the advisory lock guarantees a single owner per queue, so the
  above hold cluster-wide.

## Failure handling

- At-least-once is preserved: only successfully published (and SQS sender-fault
  poison) events are deleted; everything else stays for the next pass.
- A failing/slow queue is isolated to its own worker and bounded by
  `PUBLISH_TIMEOUT_MS` / `PG_QUERY_TIMEOUT_MS`.
- The transaction is still per (queue) batch, but batching makes the lock-hold
  window small, so we keep one tx per batch rather than introducing a lease
  column (which would change the table contract). A lease-based "publish fully
  outside the transaction" mode remains a possible future opt-in.

## Schema / indexing

- No new required columns.
- Recommended (not required) index on `(destination, id)` for fast per-queue
  selection under backlog. In steady state the table is near-empty (rows are
  deleted right after publish), so enumeration and per-queue selects are cheap
  without it.

## Configuration changes

- **Remove**: `BATCH_WORKERS`, `BATCH_MAX_SEQUENTIAL`.
- **Keep**: `BATCH_SIZE` (rows claimed per queue per pass).
- **Add (maybe)**: a cap on concurrently processed queues
  (`MAX_CONCURRENT_QUEUES`, bounded by `PG_MAX_CONNECTIONS`); a per-SQS-queue
  intra-queue concurrency knob only if needed.

## Open questions

1. **Worker-pool cap** — a fixed `MAX_CONCURRENT_QUEUES`, or "one goroutine per
   queue, bounded only by `PG_MAX_CONNECTIONS`"?
2. **Advisory lock vs. blocking** — `try`-and-skip (preferred) vs. plain blocking
   `FOR UPDATE` that serializes instances on a queue.
3. **Queue enumeration** — `SELECT DISTINCT destination` each cycle, or a cheaper
   rolling/cached view of active queues?
4. **Intra-queue SQS concurrency** — start strictly sequential per queue, and add
   bounded concurrency for standard queues only if a single queue is hot?
5. **Advisory-lock hashing** — single-key `hash(backend+destination)` (simplest,
   rare false skips) vs. two-key form to avoid collisions.
