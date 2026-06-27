# Multi-Instance Parallelization Requirements

Status: design proposal (not yet ratified for implementation).

Outboxer currently runs as a single active instance with a single connection,
holding one transaction open across the publish of a batch. This document records
the design for running multiple Outboxer instances against one outbox table with
real horizontal parallelism, while preserving ordering and the existing
at-least-once and transactional guarantees.

This is related to but separate from the LISTEN/NOTIFY wake-up
([`notify_requirements.md`](notify_requirements.md)). NOTIFY decides *when* an
instance looks; this document decides *which* instance does the work and how
order is preserved when several look at once.

## Core Principle: The Scaling Unit Is the Ordering Key

Neither backend orders a whole queue globally:

- SQS FIFO orders per `MessageGroupId`.
- Pub/Sub orders per ordering key.

So the constraint is not "one instance per queue". It is:

> A given ordering key is processed by **at most one instance at a time, in id
> order**. Different keys run in parallel.

The scaling unit is therefore the ordering key (within a destination), not the
queue or the table. A single globally-ordered stream (events that must all be
strictly ordered with no key) is inherently single-threaded and cannot be
parallelized; that is a property of ordering, not a limitation of this design.

## Claiming Model

### Unordered events (decided)

Events with no ordering key are claimed with `SELECT ... FOR UPDATE SKIP LOCKED
LIMIT n`. Multiple instances claim disjoint rows with full parallelism;
at-least-once delivery and consumer idempotency cover the rare double-send.

SKIP LOCKED is safe for a single instance too (no contention), so this is not a
mode — it is simply how claiming works.

### Ordered events (decided in principle)

Events with an ordering key are claimed under a **per-key advisory lock**:

- An instance picks a candidate key that has pending work and attempts
  `pg_try_advisory_lock(...)` keyed by a hash of `(target, destination,
  ordering_key)`. If it wins, it owns that key and processes its events in id
  order. If it loses, another instance owns the key, so it moves to a different
  one.
- Instances naturally spread across keys: serialize within a key, parallelize
  across keys.

Why advisory locks (the robustness argument):

- Advisory locks auto-release when the holding session disconnects, so a crashed
  instance releases its key locks automatically — **no stuck lease, no TTL
  reaper, no split-brain window** where two instances both believe they own a
  key.
- Postgres is the coordinator. No external consumer-group / rebalance protocol is
  needed, and there is no stop-the-world handoff to get wrong.

### Lock domain (decided)

The lock is keyed on `(target, destination, ordering_key)`, not the key alone.
The same key string on two different destinations is two independent ordering
streams and must not serialize against each other.

### Hash collisions (decided, accepted)

`hashtext`-style hashing can map two distinct keys to the same advisory lock
value. This is correctness-safe — it only over-serializes, never reorders — at a
small parallelism cost. A namespaced two-`int4` advisory lock should be used to
shrink the collision domain.

### Key cardinality (decided, non-issue)

Only keys actively being worked hold locks, so the number of distinct keys in the
table is irrelevant. Concurrency is bounded by instances × in-flight keys.

## Decoupling Publish From the Lock

Multiple instances cannot each hold a long transaction open across publishes (the
current model), because long transactions pin the global xmin horizon and
throttle autovacuum database-wide. Multi-instance therefore **requires** moving
from the current single-transaction model to a claim / publish / delete split:

1. **Claim** (short transaction): acquire the advisory lock for an ordered key
   (or `SKIP LOCKED` rows for unordered), read the pending events, commit — but
   keep the advisory lock **session-scoped** so it survives the commit.
2. **Publish** (no transaction, no row locks, does not pin xmin): send the key's
   events in id order. Cross-key publishing may run concurrently within an
   instance; within a key it is serial.
3. **Delete** (short transaction): delete the confirmed events.
4. Release the advisory lock, or loop on the same key.

Invariants that must hold (unchanged from today):

- **At-least-once:** deletes are committed only for provider-confirmed sends. On
  reclaim after a crash, unconfirmed events are re-sent and deduplicated by the
  consumer / FIFO dedup id.
- **Ordering:** because only one instance ever holds a given key's lock, and it
  publishes that key in id order, order is preserved across reclaim.
- **No double-claim:** advisory locks (ordered) and `SKIP LOCKED` (unordered)
  prevent two instances from working the same unit concurrently.

### Open question: unify or keep two models

The claim / publish / delete model also works for a single instance (just with no
lock contention). It could therefore **replace** the current single-transaction
model entirely, keeping "no modes". The alternative is to keep the simple
single-transaction model for `N=1` and use the decoupled model only for `N>1`.
Unifying is preferred for simplicity but is a larger change to a path chosen
originally for its certainty. **Decision pending.**

## Ordering Key as a Query Dimension

Ordering keys live inside the `options` JSON (`options.pubsub.orderingKey`,
`options.sqs.messageGroupId`), which is fine for publishing but not for the
claim-time selection and sharding this design needs. Selecting "the next key with
pending work" and scanning a key's events in id order must be index-supported.

Decided approach — an Outboxer-managed **generated column** derived from
`options`, so the JSON stays the single source of truth and producers still only
write `options`:

```sql
ALTER TABLE events ADD COLUMN ordering_key text
  GENERATED ALWAYS AS (
    COALESCE(options->'pubsub'->>'orderingKey',
             options->'sqs'->>'messageGroupId')
  ) STORED;

CREATE INDEX ON events (ordering_key, id) WHERE ordering_key IS NOT NULL;
```

- `jsonb ->>` and `COALESCE` are immutable, so a `STORED` generated column is
  valid.
- The partial index (`WHERE ordering_key IS NOT NULL`) only covers the keyed
  minority, keeping it cheap.
- The column is operator-installed opt-in DDL, like the optional `target` /
  `destination` columns and the DLQ table. Configurable via a column-name flag
  (proposed `EVENT_ORDERING_KEY`).
- This does **not** revert the v0.2.0 schema simplification — the operator never
  writes the column; it is derived.

### Safety constraint (decided)

To run **multiple instances** safely with **ordered** events, the `ordering_key`
column must be configured so ordered claiming can use the advisory lock. Without
it, two instances could claim different events of the same key via `SKIP LOCKED`
and publish them out of order. Therefore:

- Single instance: always safe, no `ordering_key` column required (one reader,
  id order).
- Multiple instances with only unordered events: safe without the column.
- Multiple instances with ordered events: **require** the `ordering_key` column.

This mirrors the existing "`EVENT_TARGET` required when both backends are
enabled" style of conditional requirement and should be enforced at startup where
detectable.

## Connection Model Interaction

This design pins a connection for the processing path. The implemented
LISTEN/NOTIFY feature ([`notify_requirements.md`](notify_requirements.md)) does
**not** use a dedicated listener connection: it keeps a single connection and the
listener borrows it transiently while idle. Pinning the processing connection here
therefore interacts with that listener, as the last point below describes.

- Each instance pins one `*sql.Conn` for its processor's claim/publish/delete
  work. Session-scoped advisory locks live on that pinned connection and are held
  across the publish phase; if the instance dies, the connection drops and the
  locks release automatically.
- The claim transaction, the publish (DB-idle, network only), and the delete
  transaction run sequentially on that connection. Cross-key publish concurrency
  is network concurrency and does not need additional DB connections.
- The implemented notify listener is transient: it borrows the single connection
  only while idle (see [`notify_requirements.md`](notify_requirements.md)). If this
  design pins the processing connection for session-scoped advisory locks held
  across the publish, the listener can no longer borrow that connection and would
  need its own. Reconciling the pinned processing connection with the notify
  listener (one dedicated listener connection, or listening on the pinned
  connection between phases) is an open question for the multi-instance work.

## Relationship to Static Destination Ownership

The existing `PUBSUB_DESTINATIONS` / `SQS_DESTINATIONS` ownership is a manual,
coarse-grained partition at the destination level. The advisory-lock model is the
finer-grained automatic version at the key level. They must compose:

- Destination ownership (if set) restricts which rows an instance selects at all.
- Advisory locks then arbitrate ordered keys within the selected set.
- Work-stealing (all instances share all destinations, locks arbitrate) and
  static ownership are both valid configurations; ownership is an optional
  coarse partition on top of work-stealing, not a replacement for it.

## Relationship to LISTEN/NOTIFY

- All instances `LISTEN` on the shared channel and all wake on every
  notification. This is correct: the wake-up only means "go look", and the claim
  query (locks / `SKIP LOCKED`) decides who actually works the row. The herd is a
  performance consideration, never a correctness one.
- This is why the NOTIFY design is payload-less and single-channel; see the
  Multi-Instance & Forward Compatibility section of
  [`notify_requirements.md`](notify_requirements.md).

## Open Questions / Decisions Pending

- Unify on the decoupled claim/publish/delete model for all `N`, or keep the
  single-transaction model for `N=1`.
- Exact candidate-key selection query. Note the footgun: do **not** call
  `pg_try_advisory_lock` inside a `HAVING` clause, where it acquires locks for
  every row it evaluates rather than the rows kept.
- Whether an instance claims one key per cycle or several to fill the batch
  target, and how `COLLECT_BATCH_TARGET` is spread across owned keys.
- Session-level vs transaction-level advisory lock at each phase, and explicit
  release discipline.
- Configuration surface and names (`EVENT_ORDERING_KEY`, any concurrency knobs).
- Startup validation for the "ordered + multi-instance ⇒ ordering_key required"
  constraint, and whether it is even detectable (instances do not know about each
  other).

## Non-Goals

- A built-in instance-discovery, leader-election, or rebalance protocol. Postgres
  locks are the coordinator.
- Logical-replication / CDC as the claiming source. That is a separate future
  track.
- Cross-database or cross-table sharding.

## Acceptance Scenarios

The test plan lives in
[`multi_instance_scenarios.md`](multi_instance_scenarios.md).
