# Configuration Surface Audit

Status: audit and reduction plan, written 2026-07-09. Grounded in the July
2026 cloud benchmark campaign and the project's configuration philosophy:
decide once, ship the good value as the behavior, and treat knobs as
temporary instruments for finding that value — not as a way to defer the
decision to the user.

The relay currently exposes 44 settings. This audit sorts them into four
buckets. The target end state is roughly 30, of which the large majority are
deployment identity that no philosophy can remove.

## Bucket 1 — Keep: deployment identity and schema mapping

These describe *this deployment*, not relay behavior. There is no universal
value to decide.

- Schema mapping (the adapt-to-existing-tables feature):
  `EVENT_TABLE`, `EVENT_ID`, `EVENT_TIMESTAMP`, `EVENT_PAYLOAD`,
  `EVENT_TARGET`, `EVENT_DESTINATION`, `EVENT_OPTIONS`
- Database identity and trust: `PG_HOST`, `PG_PORT`, `PG_USER`,
  `PG_PASSWORD`, `PG_DATABASE`, `PG_SCHEMA`, `PG_SSL`,
  `PG_SSL_REJECT_UNAUTHORIZED`, `PG_SSL_ROOT_CERT`
- Provider topology: `PUBSUB_ENABLED`, `SQS_ENABLED`,
  `DEFAULT_PUBSUB_TOPIC`, `DEFAULT_SQS_QUEUE_URL`, `PUBSUB_PROJECT_ID`,
  `AWS_REGION`, and the sharding selectors `PUBSUB_DESTINATIONS` /
  `SQS_DESTINATIONS`
- Cross-cloud auth topology: `AWS_ROLE_ARN`, `AWS_ROLE_SESSION_NAME`,
  `AWS_WEB_IDENTITY_PROVIDER`, `AWS_WEB_IDENTITY_AUDIENCE`
- Emulator/regional endpoints (required by the integration tests and by
  Pub/Sub ordering keys): `PUBSUB_API_ENDPOINT`, `SQS_API_ENDPOINT`
- Failure-mode policy (product semantics, deployment-specific):
  `DLQ_TABLE`, `MAX_EVENT_AGE_MS`
- Operational wiring: `HEALTH_PORT` (0 = off), `LOG_LEVEL`, `LOG_FORMAT`
- Provisioning (init only): `PG_INIT_USER`, `PG_INIT_PASSWORD`,
  `PG_PRODUCER_ROLES`

## Bucket 2 — Hardcode now: the decision already exists

Measurement or design reasoning has already picked the value; the knob only
advertises indecision. Delete the flag/env, keep the constant with a comment
stating where the value came from.

| Setting | Hardcode to | Basis |
| --- | --- | --- |
| `SQS_SEND_CONCURRENCY` | 128 | Measured on Fargate/EKS; pool is sized to it; idle deployments pay nothing for headroom. |
| `BACKLOG_COUNT_LIMIT` | 100,000 | Probe measured at ~10–20 ms at the cap; bounded by construction. No tuning story. |
| `ERROR_COOLDOWN_MS` | 5 s | Retry hygiene; interacts with nothing a user can reason about better than we can. |
| `PUBLISH_RESULT_GRACE_MS` | 5 s | Internal async-result plumbing. |
| `WATCHDOG_INTERVAL_MS` | 10 min | Internal liveness plumbing. |
| `STATS_INTERVAL_MS` | 10 s | Log cadence; also paces the backlog probe. 0-disables adds a code path nobody needs. |
| `PG_CONNECT_TIMEOUT_MS` | 10 s | Connection plumbing. |
| `AWS_ROLE_DURATION_SECONDS` | 1 h | STS plumbing. |
| `AWS_CREDENTIAL_REFRESH_WINDOW_MS` | 5 min | STS plumbing. |
| `HEALTH_STALE_AFTER_MS` | 5 min | Anti-flap window; 5 min is a universal answer given that provider failures never flip health. (Added as a knob 2026-07-08; superseded by the no-knobs philosophy.) |
| `NOTIFY_CHANNEL` | derived: `outboxer_events_<event table>` | Removes a multi-table coordination footgun; init and relay derive identically. Breaking for non-default tables — needs an init re-run, called out in release notes. |

## Bucket 3 — Benchmark, then hardcode

| Setting | Current | Question | Plan |
| --- | --- | --- | --- |
| `COLLECT_BATCH_TARGET` | 5,000 | Is 5k the throughput sweet spot? Larger batches amortize per-batch overhead but hold row locks and publish windows longer. | Sweep 1k / 2k / 5k / 10k / 20k on GKE (Pub/Sub, 0.1% run variance) and EKS (SQS), definitive methodology, `kubectl set env` between runs. Hardcode the winner unless the curve is flat — a flat curve also justifies hardcoding 5k. |
| `POLL_INTERVAL_MS` | 1 s | Backstop cadence. The busy-loop A/B already vindicated notify + backstop; the persistent-listener change (see pipelined-batches spec discussion) makes the backstop rarer still. | No further benchmark. Hardcode 1 s when the persistent listener lands. The 0 = busy-poll mode was a benchmark instrument; it goes with the knob. |

## Bucket 4 — Decide alongside parked specs

- `PUBLISH_TIMEOUT_MS` (30 s) and `PG_QUERY_TIMEOUT_MS` (30 s): both are
  entangled with `specs/batch_budget_requirements.md` (time budgets per
  batch) and `specs/collection_plan_requirements.md` (the stall crosses
  PG_QUERY_TIMEOUT). Deciding them before those designs land would decide
  them twice. They are the last two candidates for removal, not the first.

## Sequencing

1. Run the Bucket 3 batch-target sweep (this campaign).
2. Remove Bucket 2 + `COLLECT_BATCH_TARGET` + `POLL_INTERVAL_MS` in one
   commit each (flag, env var, docs row, validation), with the chosen
   constants and their provenance comments.
3. `docs/configuration.md` shrinks to identity + policy settings, one page.
4. Bucket 4 falls out of the batch-budget / collection-plan work later.

Breaking-change note: this is a pre-1.0 project; removed env vars should
fail loudly at startup (unknown OUTBOXER-relevant vars in the environment
are currently ignored — consider warning on the removed names for one
release) and be listed in the release notes.
