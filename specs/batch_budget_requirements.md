# Batch Time Budget Requirements

Status: design proposal, not yet accepted. Written 2026-07-06 after a
production-readiness review; revisit before implementing.

## Problem

The whole batch — `SELECT ... FOR UPDATE`, publish every event, insert dead
letters, delete finished rows — runs inside one PostgreSQL transaction. Its
duration is therefore `events × publish latency`:

- 5,000 events at 10ms each ≈ 50s per transaction. Normal.
- A single-key ordered Pub/Sub group publishes strictly sequentially with a
  30s publish timeout; a degraded provider can stretch one batch to hours.

A long transaction is a database-wide hazard, not just a relay concern:

- It pins the xmin horizon, so autovacuum cannot reclaim dead tuples in **any**
  table of the database while the batch runs.
- The outbox table is autovacuum's worst customer already (every row is
  inserted and deleted once), so it bloats fastest exactly when transactions
  are longest.
- The `FOR UPDATE` locks block any overlapping relay (rolling deploys) for the
  full duration.

Today the only mitigation is lowering `COLLECT_BATCH_TARGET`, which is
undocumented as such and trades throughput permanently to bound a tail case.

## Invariants that must be preserved

1. **At-least-once delivery.** Events are deleted only in the same transaction
   that observed their confirmation (or dead-lettered them). Crash at any
   point re-sends, never loses.
2. **Inter-relay exclusion.** The selection's `FOR UPDATE` locks are what make
   two relays with overlapping ownership (rolling deploys, misconfigured
   replicas) serialize politely instead of double-publishing.
3. **No schema additions.** The relay only needs SELECT/DELETE (plus the
   column-level UPDATE grant for `FOR UPDATE`); producers' tables gain no
   Outboxer bookkeeping columns.
4. **Single database connection**, with the idle `LISTEN` undisturbed.
5. **Sender semantics.** In particular: an ordered Pub/Sub publish whose
   outcome is ambiguous (`context.DeadlineExceeded`) escalates to
   `ErrFatalAfterCommit` and restarts the relay. Any new mechanism must not
   trigger this path routinely.

## Rejected designs

### Continuous commit — delete/DLQ each event as its confirmation arrives

The intuitive fix, and the reason this document exists. It is impossible under
the current locking model:

- PostgreSQL has no partial commit: work inside the batch transaction becomes
  visible only when the whole transaction commits. Subtransactions do not
  release locks or publish results.
- Doing the deletes on a **second connection** does not work either: locks are
  per-transaction, so the delete would block on the batch's own `FOR UPDATE`
  locks like any other transaction.

Continuous commit therefore requires *not holding the selection lock across
the publish phase*, which breaks invariant 2 or 3 (below).

### Lock-free selection with short delete transactions

Select without `FOR UPDATE`, commit immediately, publish, delete confirmed
rows in short transactions. Bounded transactions, but overlapping relays now
double-publish **everything they both see, continuously** — today they merely
serialize. Duplicates are within at-least-once, but a duplicate storm on every
rolling deploy is a serious behavioral regression.

### Claim column

`UPDATE ... SET claimed_by/claimed_until` in a short transaction, publish
without locks, delete in another short transaction. Sound, bounded, and how
many queue-on-Postgres systems work — but it adds an Outboxer bookkeeping
column to the producer's table (invariant 3), needs claim-expiry semantics for
crashed relays, and changes the grants story. A much bigger design change than
the problem warrants.

### Adaptive batch sizing

Size the next batch from the observed publish rate so transactions *converge*
on a duration target. No structural change, but it is reactive: the first
pathological batch (the one that hits a degraded provider) is still unbounded,
and that is precisely the case that matters.

### Documentation only

Describe the `COLLECT_BATCH_TARGET` trade-off and vacuum implications. Cheap,
keeps the hazard.

## Proposed design: batch time budget

Bound the batch by **time** instead of only by row count. When the budget
expires, senders stop *dispatching new events*; in-flight publishes complete
normally; the batch then dead-letters and deletes what was confirmed and
commits. Undispatched events stay in the table and the next batch re-selects
them immediately — re-acquiring locks, so inter-relay exclusion is never
weakened. This is "commit what we got", realized at time boundaries instead of
per event, which is the closest the current locking model allows.

Transaction duration becomes bounded by:

```
select time + BATCH_TIME_BUDGET_MS + one in-flight publish tail
            + DLQ/delete time
```

where the publish tail is at most `PUBLISH_TIMEOUT_MS` (+ grace for Pub/Sub).

### The cut must be cooperative, not a context cancellation

Cancelling the dispatch context on budget expiry would surface as
`context.DeadlineExceeded` inside publishes, which the ordered Pub/Sub path
deliberately escalates to `ErrFatalAfterCommit` (ambiguous outcome → restart).
A routine budget expiry must not restart the relay.

Instead, providers receive a `should I keep dispatching?` check (natural home:
`provider.Callbacks`, next to `MarkProgress`) and consult it:

- Pub/Sub ordered: between events within a group loop.
- Pub/Sub unordered: in the publish-initiation loop (the await phase is
  already bounded by the publish timeout).
- SQS: between chunks (standard) and between per-group sends (FIFO).

An event never dispatched is simply never reported, which is the existing
kept-for-retry path: no new error variants, no new failure modes. A cut
ordered group resumes next batch exactly after its confirmed prefix, so
ordering is preserved.

### Existing machinery this reuses

- Partial commit already exists: a batch with sender errors commits confirmed
  work and keeps the rest. The budget produces the same shape of result.
- `batchDrained` and the backlog gauge stay correct without changes: a cut
  batch either filled its selection (not drained → probe path) or its kept
  events are exactly the remaining backlog (drained → exact path).
- `MAX_EVENT_AGE_MS` expiry, the watchdog, and `/healthz` are unaffected: the
  budget produces *more frequent* commits, which only helps them.

### Observability

- Info log when a budget cut happens: events dispatched vs. kept.
- Counter metric (e.g. `outboxer_batch_budget_cuts_total`) so sustained
  slowness is visible on dashboards.

## Open questions (decide before implementing)

1. **Default on or off?** Proposal: on, at `BATCH_TIME_BUDGET_MS=60000`.
   60s never triggers on healthy workloads (5,000 events would need >12ms
   average latency) and caps the pathological case at ~1.5 minutes of
   transaction time. `0` = unbounded (today's behavior). The conservative
   alternative is default `0` with loud documentation.
2. **Validation.** A budget below `PUBLISH_TIMEOUT_MS` means a batch may
   dispatch only one slow event before cutting — legal and self-correcting,
   but probably a misconfiguration. Reject, warn, or document?
3. **Interaction with the (also pending) per-route backoff design.** They
   compose: the budget bounds transaction *duration*, per-route backoff bounds
   retry *rate* against failing destinations. A budget-cut batch keeps
   events for reasons that are not the destination's fault, so budget cuts
   must not feed backoff accounting.

## Companion change, independent of the decision

`docs/deployment.md` should gain a vacuum/bloat section regardless: the outbox
table churns 100% of its rows, so recommend per-table autovacuum tuning
(lower `autovacuum_vacuum_scale_factor`, consider `autovacuum_vacuum_cost_limit`),
note the empty-but-bloated table pattern, and — once this design lands —
reference the budget as the mechanism that keeps vacuum unblocked.

## Test scenarios (sketch)

| ID | Scenario | Expected |
| --- | --- | --- |
| BUDGET-01 | Batch completes within the budget. | Behavior identical to today; no cut, no log. |
| BUDGET-02 | Ordered Pub/Sub group slower than the budget. | Batch commits the confirmed prefix, keeps the rest; no `ErrFatalAfterCommit`; next batch resumes the group in order. |
| BUDGET-03 | SQS standard queue, budget expires between chunks. | Dispatched chunks confirmed and deleted; remaining chunks kept. |
| BUDGET-04 | Budget expires with an in-flight publish. | The in-flight publish completes and is committed; only undispatched events are kept. |
| BUDGET-05 | `BATCH_TIME_BUDGET_MS=0`. | Unbounded, byte-identical behavior to today. |
| BUDGET-06 | Budget cut with a DLQ configured and poison in the dispatched prefix. | Poison dead-lettered and deleted in the same commit as the confirmed events. |
| BUDGET-07 | e2e: slow FIFO group with a small budget across several batches. | All events delivered exactly like an unbudgeted run, spread over more transactions; per-transaction duration observably bounded. |
| BUDGET-08 | Crash between budget-cut commits. | Standard at-least-once: only the uncommitted remainder re-sends. |
