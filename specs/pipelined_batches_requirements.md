# Pipelined Batches Requirements

Status: design proposal, not yet implemented. Written 2026-07-09 from the
definitive cross-cloud benchmark (`test/cloud/results/`, July 8–9 runs).

## Motivation

The definitive benchmark measured the per-batch phase split with the
`outboxer_last_batch_db_seconds` / `_publish_seconds` gauges (median run,
5,000-event batches):

| Platform | Database (select · delete) | Publish | Serial cycle |
| --- | --- | --- | --- |
| GKE → Pub/Sub | 0.158 s | 0.077 s | 0.234 s |
| Cloud Run → Pub/Sub | 0.165 s | 0.072 s | 0.237 s |
| EKS → SQS | 0.104 s | 0.143 s | 0.246 s |
| Fargate → SQS | 0.113 s | 0.177 s | 0.289 s |

The phases run strictly one after another, batch after batch: while the relay
publishes batch N, the database sits idle; while it selects batch N+1, the
provider sits idle. Overlapping the database work of the next batch with the
publish of the current one bounds the steady-state cycle by
`max(select, publish, delete)` instead of their sum.

Projected from the measured phases (assuming the publish stage stays serial
and delete overlaps the successor's select): roughly +45% throughput on the
Pub/Sub platforms and +60–70% on the SQS platforms. Not a doubling — that
would require perfectly equal phases — and **idle-state latency is unchanged**
(a single event has nothing to pipeline with; its path is still
notify → select → publish). What does improve besides throughput is event
sojourn time under sustained load, in proportion to the throughput gain.

## Why the current design serializes

`processOneBatch` opens one transaction on the relay's single connection:
`SELECT ... FOR UPDATE` claims the batch, the providers publish while the
row locks are held, then `DELETE ... = ANY($1)` and `COMMIT`. This buys the
core guarantees with zero bookkeeping:

- **At-least-once**: a crash at any point releases the locks and leaves the
  rows; the next relay run re-collects them. No claim columns, no reclaim
  timeouts, no lease logic.
- **Per-key ordering across retries**: a failed batch rolls back and is
  re-collected *before* any newer events, because collection is
  `ORDER BY id` and nothing else is in flight.

Any pipelining design MUST preserve both properties unchanged. Designs that
commit a claim before publishing (delete-then-publish, claimed_at columns)
are rejected: they either downgrade to at-most-once on crash or reintroduce
the lease machinery this project deliberately avoids.

## Design: double-buffered batches over two connections

Two batch workers, each owning a dedicated database connection and the full
transaction lifecycle of its batch (select → publish → dead-letter → delete →
commit). The pipeline is the two workers alternating, with two rules:

1. **Collection skips locked rows.** The per-route pick adds
   `FOR UPDATE SKIP LOCKED`, so worker B's select ignores the rows worker A
   holds and claims the next events in id order. (Today's plain `FOR UPDATE`
   would simply block B on A's locks, serializing everything again.)
2. **Publishes are serialized in collection order.** A token passes from
   batch to batch: worker B may start publishing only after worker A's
   publish phase completes (not its commit — B's publish may overlap A's
   delete+commit). This preserves per-key ordering when one ordering key
   spans consecutive batches.

The steady-state timeline:

```
conn A:  [select N   ][publish N   ][delete N]
conn B:              [select N+1   ]          [publish N+1 ][delete N+1]
conn A:                             [select N+2]            ... 
```

### Failure rule

If batch N fails (publish error, database error — anything that rolls back
its transaction), any successor batch that has been collected but has not
started publishing MUST also roll back, unpublished, and the pipeline
restarts from collection after the error cooldown. This is what preserves
retry-before-newer ordering: N's events return to the table and are
re-collected ahead of N+1's, exactly as today. Aborting a collected batch is
free — nothing was published, the rollback releases the locks.

A crash kills both connections; both batches' locks vanish and all rows
remain. At-least-once is untouched.

### Scope

- **No permanent knob.** Per the project's configuration philosophy (decide
  once, ship the good value as the behavior), pipelining is validated with a
  temporary flag on a branch against the cloud harness and, if it meets the
  acceptance bar, becomes the only mode; the flag is deleted before release.
  Pipeline depth is fixed at 2: the serial publish stage is the pipeline's
  backbone, and a third in-flight batch would only add abort-on-failure
  surface.
- Pipelining engages only when the previous select filled the batch target
  (the existing `batchDrained` signal). A relay trickling single events keeps
  one connection's cadence and pays nothing new — this adaptivity is behavior,
  not configuration.
- The LISTEN/notify connection, watchdog, health, and metrics endpoints are
  unchanged. The stats accounting must tolerate two in-flight batch results
  (`addCommittedBatch` is already commit-ordered).

### Interactions

- **Connection budget.** This adds one connection per relay. The
  single-connection requirement is retired (2026-07-09) in favor of a small,
  fixed, documented budget — batch connection + listener + one pipeline
  connection — stated as a formula in the deployment docs so sharded
  operators can do the arithmetic. The observability constraint (no second
  connection *for metrics*) is unaffected.
- **Collection plan spec.** `specs/collection_plan_requirements.md` reworks
  the collection query into a bounded cursor walk. SKIP LOCKED composes with
  it (it is a lock-clause modifier, not a plan shape), but the two changes
  touch the same query builder and should land in that order: plan fix
  first, pipelining second, so pipeline benchmarks are not polluted by plan
  variance.
- **Multi-instance.** SKIP LOCKED incidentally removes lock-wait contention
  between relays sharing a table (`specs/multi_instance_requirements.md`
  scenarios) — batches interleave instead of queueing. A welcome side effect,
  but multi-instance correctness must not *depend* on pipelining being on.
- **DLQ.** Dead-letter inserts stay inside each batch's own transaction;
  nothing changes.

## Acceptance

Measured on the cloud harness with the definitive methodology
(single-transaction analyzed load, 1 s sampling, three runs):

1. Median throughput with `BATCH_PIPELINE=2` improves by ≥ 40% over the
   July 2026 baseline table on at least one Pub/Sub platform and one SQS
   platform, with no batch or sender errors.
2. Idle-state latency (p50/p99) is unchanged within noise — the pipeline must
   not add wake-up or token overhead to the single-event path.
3. The ordered-events smoke scenario passes with ordering keys deliberately
   spanning batch boundaries under continuous insert load.
4. The crash-recovery e2e test (kill mid-publish) passes with the pipeline
   on: no lost events, duplicates only within at-least-once bounds.
5. A forced publish failure in batch N with batch N+1 already collected
   results in N's events being re-delivered before N+1's (per key), and N+1's
   rows never reaching the provider before their retry turn.
