# Collection Query Plan Robustness Requirements

Status: implementation design settled 2026-07-09 (see "Implementation
design" below); originally written 2026-07-08 from live GCP Cloud Run
performance runs (`test/cloud/gcpcloudrun`, reports in
`test/cloud/results/`). The stall described here is considered a bug: the
collection query must not have a statistics-dependent degenerate case.

## Implementation design (2026-07-09)

The cursor walk from the fix requirements, refined by one discovery made at
design time: **a bounded scan alone can starve routes**. The
broken-destination semantic (a failing route's growing backlog must not
block healthy routes — pinned by the
`RouteBrokenDestinationDoesNotBlockHealthyRoute` e2e scenario) is served
today by the routes CTE's unbounded discovery scan. A pure oldest-N walk
with a scan bound would let a broken route with a deeper-than-the-bound
backlog occupy the whole scan window every batch, starving routes behind
it. The design therefore has a cheap common path and a targeted
pathological path:

1. **Cursor walk** (common path). Page through the PK index in id order:
   `WHERE id > $cursor ORDER BY id LIMIT $page`, with the routable
   predicate applied to the page's rows and the resolved route expressions
   computed per row. The relay accumulates ids per route, capped at a
   per-route quota `max(1, target / knownRoutes)`, and stops when the batch
   target fills, the table ends, or a fixed 100k scan bound is reached
   (same reasoning and constant as the backlog probe). In a healthy
   deployment this is a single bounded statement.
2. **Route memory + targeted probes** (pathological path). Routes that
   yielded rows in the previous batch stay known. A known route the walk
   did not reach gets its own bounded pick — today's per-route LATERAL
   shape for exactly one route (`WHERE routable AND route = ... ORDER BY id
   LIMIT quota`). Healthy deployments never probe; a broken-route pileup
   costs one targeted index walk per starved route per batch. Known
   limitation, documented: a route *born* behind a deeper-than-100k broken
   backlog is not discovered until that backlog drains (DLQ / MAX_EVENT_AGE
   are the mitigations for the backlog itself).
3. **Lock at fetch.** The final statement fetches and locks the full rows:
   `WHERE id = ANY($ids) ORDER BY id FOR UPDATE` — the id-array shape
   already proven index-safe by `deleteEvents`.
4. **`SET LOCAL enable_seqscan = off`** on the batch transaction. Postgres
   has no plan hints; this is the structural guarantee that none of the
   above can degrade to a seq-scan+sort plan under stale statistics. All
   batch statements are id-keyed, so the setting costs nothing when
   statistics are healthy.
5. **Per-route order is preserved** (each route's ids are always taken
   oldest-first, by walk order or probe order), which is what ordered
   delivery actually requires; global cross-route order was never
   guaranteed. `batchDrained` is re-derived: a batch is drained when the
   walk reached the end of the table with no route capped by quota and no
   probes pending.

## Symptom

Three 200,000-event bulk-load performance runs against the deploy/gcp-cloudrun
stack (Cloud SQL Postgres 17, 4 vCPU / 16 GB; events: 256 B payloads,
unordered, single route):

| Run | Relay CPU | Drain | Avg throughput | Behavior |
| --- | --- | --- | --- | --- |
| 1 | 2 vCPU | 46 s | 4,360/s | 30 s stall, one batch error, then ~20k/s |
| 2 | 4 vCPU | 46 s | 4,391/s | identical stall — CPU is not the bottleneck |
| 3 | 2 vCPU | 20 s | 9,827/s | no stall; collect and delete phases of 4–6 s each |

The stall: the sent counter freezes right after the bulk COPY completes, one
batch fails after exactly `PG_QUERY_TIMEOUT` (30 s) with
`database batch error: timeout: context deadline exceeded`, the batch rolls
back, the error cooldown passes, and every subsequent batch runs at the
steady-state ~230 ms. Self-healing, bounded, and reproducible in 2 of 3 runs.
Steady-state throughput after the table settles is ~18–20k events/s.

## Evidence chain

1. **Stale-statistics plan** (captured on both local Postgres 17 and Cloud
   SQL; byte-identical): after a bulk COPY into a previously tiny table, the
   planner estimates 1,759 distinct routes (actual: 1) and ~1,800 routable
   rows (actual: 200,000), because the routing predicates are expressions
   (`COALESCE`/`NULLIF` over target/destination) that get default selectivity
   guesses without column statistics. Under that plan the per-route pick is a
   **full Seq Scan + Sort + Limit** instead of a bounded PK-index walk, and
   the routes CTE seq-scans the table again.
2. **The plan alone is not the stall**: executing that exact plan on the same
   200,000 stale-stats rows takes 241 ms on Cloud SQL (569 ms locally).
3. **The dynamic ingredient is the concurrent bulk load**: `pg_stat_activity`
   sampling at 1 Hz during run 3 shows the collection query active and
   **CPU-bound for 4–6 s** (wait_event NULL — no locks, no I/O waits) and,
   unexpectedly, the batched `DELETE ... = ANY($1)` also CPU-bound for ~6 s,
   while the COPY ingest competes for database CPU.
4. Combined: seq-scan-heavy plans × CPU contention with the ingest stretch
   batch database calls to seconds, with enough variance that the tail
   occasionally crosses `PG_QUERY_TIMEOUT`. Once autoanalyze lands
   (asynchronously, up to `autovacuum_naptime` after the load), plans return
   to index-driven and batches to ~230 ms.

## Why this is a bug and not an operations problem

Bulk backfills into the outbox are a normal producer operation (migrations,
replays, batch jobs). The relay stalling for one query-timeout per backfill —
or longer, when analyze is delayed — is a degenerate case of the collection
query's plan-sensitivity, not of the environment. The recovery path (timeout →
rollback → cooldown → retry) works exactly as designed, but the stall is
avoidable by construction.

## Insufficient remedies (document, but do not consider fixes)

- **Per-table autovacuum tuning from `init`** (`autovacuum_analyze_scale_factor`
  etc.): correct hygiene for a 100%-churn table and worth adding regardless,
  but analyze remains asynchronous with up-to-naptime latency — it shrinks the
  window, never closes it.
- **Operator `ANALYZE` after backfills**: deterministic and cheap; belongs in
  the docs regardless. But relying on operator discipline for relay liveness
  is not a fix.
- **Tuning `PG_QUERY_TIMEOUT`**: changes the stall's duration, not its
  existence.

## Fix requirements

The goal: the collection flow's **cost class must not depend on planner
statistics**. Bounded work by construction, whatever the table's state.

1. The per-route pick must be unable to degrade to a full scan + sort. The
   planner chooses seq+sort today because, under garbage selectivity
   estimates, walking the PK index "never" reaches the LIMIT. No Postgres
   hint mechanism exists, so robustness must come from query structure.
2. The most promising direction is replacing the single monolithic statement
   (routes CTE + per-route lateral picks + rejoin) with a **cursor-driven
   index walk**: page through the table in id order
   (`WHERE id > $cursor ORDER BY id LIMIT $page` plus the routable
   predicate), accumulating per-route quotas relay-side, stopping when quotas
   fill or a scan bound is reached. Properties: every statement is a bounded
   PK-index range scan by construction; the routes-discovery full scan
   disappears entirely; the scan bound composes with the same reasoning as
   the backlog probe.
3. Row locking (`FOR UPDATE`) and the fetch of full rows can reuse the
   id-array shape already proven index-safe by `deleteEvents`
   (`WHERE id = ANY($1)`), locking at fetch time inside the batch
   transaction.
4. Semantics that must be preserved and need explicit design attention:
   - **Per-route fairness**: today the share is `target / count(pending
     routes)` computed by the routes CTE. A cursor walk discovers routes as
     it scans, which changes how shares are allocated; the requirement is
     that no eligible route starves, not that the current formula is
     reproduced exactly.
   - **Inter-relay exclusion** (`FOR UPDATE` before dispatch) and
     at-least-once semantics, unchanged.
   - **Ownership filters and disabled-target exclusion**, unchanged.
   - **Head-of-line behavior**: a large block of rows belonging to other
     relays (sharding) or to one saturated route must not consume the whole
     scan bound; interaction with `batchDrained`/backlog reporting must be
     re-derived.
5. The slow batched DELETE observed during the load window (CPU-bound ~6 s)
   must be re-measured once the collection fix lands; if it persists, it is
   its own issue.
6. **Sharding-aware access paths.** Destination ownership is a row filter
   today, so with K sharded relays each relay's scan wades through the other
   shards' rows: K-way sharding pays roughly K× scan overhead instead of
   partitioning the work. `init` knows each deployment's owned destinations,
   so it can generate **per-shard partial indexes**
   (`... WHERE destination IN (...)`) that make each relay's index walk
   O(own pending). The redesigned collection flow should be able to use such
   an index when it exists; whether `init` creates them by default or via a
   flag is a design choice for implementation time.

## Regression test

Local, no cloud required, and must fail against today's query:

- Compose Postgres; create the schema via `init`; `ALTER TABLE events SET
  (autovacuum_enabled = off)` to pin statistics stale permanently.
- Bulk COPY 200k rows, run the relay, and assert the outbox drains with
  **zero batch errors** and no single collection call exceeding a small bound
  (a few seconds), despite the stale statistics.
- The cloud perf scenario (`just cloud-gcp-cloudrun-perf`) then serves as the
  end-to-end validation: drain time for 200k should become consistent
  (~20 s), where today it varies 20–46 s depending on whether the stall
  fires.

## Acceptance benchmark: sharded scaling

The strategic payoff of a statistics-immune, sharding-aware collection flow
is that the polling outbox scales **out** per database, which log-based CDC
cannot: a logical replication slot is decoded serially by a single walsender,
and additional slots each re-decode the entire WAL. Whether Outboxer actually
converts shards into throughput is unproven, so the fix's acceptance
benchmark is a multi-relay scenario in the cloud harness, sequenced after the
collection fix (which decides whether sharding scales at all):

- **Setup**: N relay services (start with N=4) over one outbox table, each
  owning a disjoint destination set (`PUBSUB_DESTINATIONS` split across N
  topics), per-shard partial indexes provisioned. Deploy stack gains a shard
  count variable; the harness produces events across all shards and samples
  every relay's `/metrics`.
- **Measurement**: same bulk-load drain profile as the single-relay runs
  (N × 100k events, 256 B, unordered), reporting aggregate events/s and the
  per-shard curves.
- **Success criteria**: aggregate throughput ≥ 3× the single-relay baseline
  at N=4 (sublinear from shared table churn is expected; worse than 3×
  means cross-shard interference still dominates); no shard starves; each
  relay's `outboxer_backlog_events` reports only its own shard; drain time
  is consistent across runs (the stall variance this document exists for
  stays gone).
- **Context for the numbers**: the single-relay baseline is ~18–20k
  events/s; well-tuned single-connector Debezium setups are commonly cited
  in the ~30–80k events/s range and cannot shard within one database. The
  benchmark's purpose is to make "exceeds the per-database ceiling of
  log-based CDC" a measured claim instead of an architectural argument.

## Reference numbers (2026-07-08, gcp-cloudrun stack)

- Steady state: ~230 ms per 5,000-event batch, ~18–20k events/s sustained;
  identical at 2 and 4 relay vCPUs (the batch loop alternates DB and publish
  phases serially — CPU is not the limiter).
- Bulk insert: 200k rows via COPY through cloud-sql-proxy in ~18 s.
- Full drain of 200k: 20 s (no stall) to 46 s (with stall).
