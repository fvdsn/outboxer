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

There is no central dispatcher. A fixed pool of **`MAX_CONCURRENT_QUEUES`
self-dispatching workers** each find and drain a queue:

**Worker loop**
1. Enumerate candidate queues with pending events
   (`SELECT DISTINCT destination …`, refined per backend/target).
2. Walk candidates and `pg_try_advisory_xact_lock` each; take the first one
   acquired (skipping queues already owned by another worker or instance).
3. `SELECT … WHERE destination = $1 ORDER BY id LIMIT X FOR UPDATE` — the queue's
   next batch, in order.
4. Publish via the backend (see below).
5. Delete the events that were published (or are poison/dropped).
6. Commit (releases the advisory lock); repeat for the same queue until it
   drains, then go back to step 1.

Self-dispatch (rather than a central dispatcher) means a worker uses exactly one
connection for its own enumeration + claim + publish + delete, so there is no
separate dispatcher connection to account for. The enumeration query is cheap
because a healthy outbox table is near-empty.

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
- **SQS backend** — chunk into 10s and `SendMessageBatch`. The AWS SDK does no
  batching or concurrency of its own, so we do it: a queue worker sends its
  batches concurrently, partitioned by message group (standard queues = every
  message is its own group → full concurrency; FIFO = same-group batches stay
  sequential, different groups parallel). All SQS sends — across every queue
  worker — share **one global semaphore** (`SQS_SEND_CONCURRENCY`), so the total
  in-flight `SendMessageBatch` count is stable and independent of how many queues
  are active. A single hot queue can use the whole budget; many queues share it.

`parallelizeEvents`, `BATCH_WORKERS`, `BATCH_MAX_SEQUENTIAL`, and `strHash` are
removed.

### Why SQS has a concurrency knob and Pub/Sub does not

We own SQS sending and the SDK gives no built-in bound, so without a knob it is
either sequential or unbounded — hence a single global `SQS_SEND_CONCURRENCY`.
Pub/Sub is the opposite: the client manages its own concurrency
(`NumGoroutines`) and applies **flow control** (max outstanding messages/bytes)
for backpressure. A Pub/Sub equivalent would be per-topic (`NumGoroutines`),
which would reintroduce the `topics × N` multiplication we explicitly avoid for
SQS, and a stable global bound would mean gating in front of the client and
fighting its batching. So Pub/Sub is left to its client; if tuning is ever
needed, the right lever is flow-control settings, not a goroutine count.

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

## Connection model

Connections are a function of concurrency only. Each worker uses exactly one
connection for its whole pass (enumerate → advisory-lock → claim → publish →
delete → commit), held across the publish because the advisory lock and the
`FOR UPDATE` rows must stay locked until commit. Nothing else touches the
database (the watchdog and health endpoint do not; `checkDBWorks` runs once at
startup).

So:

```
connections in use = MAX_CONCURRENT_QUEUES
```

Because the count is fully derived, **`PG_MAX_CONNECTIONS` is removed**: the pool
size is set internally to `MAX_CONCURRENT_QUEUES` (with `MaxIdleConns` tracking it
so connections stay warm). The operator provisions `MAX_CONCURRENT_QUEUES`
sessions on the server and that is the whole story. Setting
`MAX_CONCURRENT_QUEUES=1` reproduces today's single-connection, one-queue-at-a-time
footprint (still correctly ordered and isolated, just without cross-queue
parallelism).

The only thing that would decouple connections from in-flight queues is the
lease model (claim in a short tx, release the connection, publish without one,
delete in another tx) — deferred, since it needs a schema column.

## Schema / indexing

- No new required columns.
- Recommended (not required) index on `(destination, id)` for fast per-queue
  selection under backlog. In steady state the table is near-empty (rows are
  deleted right after publish), so enumeration and per-queue selects are cheap
  without it.

## Configuration changes

- **Remove**: `BATCH_WORKERS`, `BATCH_MAX_SEQUENTIAL`, `PG_MAX_CONNECTIONS`.
- **Keep**: `BATCH_SIZE` (rows claimed per queue per pass).
- **Add**:
  - `MAX_CONCURRENT_QUEUES` — number of self-dispatching workers; also the
    connection-pool size (set internally).
  - `SQS_SEND_CONCURRENCY` — global cap on in-flight `SendMessageBatch` calls
    across all queues.
- **Not added**: a Pub/Sub send-concurrency knob (left to the client's flow
  control — see above).

## Decisions (resolved)

- **Worker model**: a fixed pool of `MAX_CONCURRENT_QUEUES` self-dispatching
  workers, no central dispatcher.
- **Connections**: derived (`= MAX_CONCURRENT_QUEUES`); `PG_MAX_CONNECTIONS`
  removed.
- **Queue exclusivity**: `try`-advisory-lock and skip (no blocking `FOR UPDATE`
  contention between instances).
- **SQS concurrency**: one global semaphore, group-partitioned within a worker;
  no per-queue knob.
- **Pub/Sub concurrency**: left to the client; no knob.

## Open questions

1. **Queue enumeration** — `SELECT DISTINCT destination` each cycle, or a cheaper
   rolling/cached view of active queues under heavy backlog?
2. **Advisory-lock hashing** — single-key `hash(backend+destination)` (simplest,
   rare harmless false skips) vs. two-key form to avoid collisions.
3. **Worker idle behavior** — when no queue can be locked (all owned or empty),
   how long/how to back off before re-enumerating (ties into `POLL_INTERVAL_MS`).
