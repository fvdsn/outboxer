# Init / Provisioning Command Scenarios

Status: implementation test plan.

These scenarios accompany [`init_requirements.md`](init_requirements.md). They
exercise the SQL generator offline (print mode, golden output, no database) and
the apply path against real Postgres. Layer hints: generation and config-driven
object selection are unit-testable from the generator; role creation, grants, and
idempotency are integration tests against real Postgres; the round-trip invariant
(provision, then boot the relay on the same config) is an e2e smoke test against
the real binary.

## Dispatch & Modes

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-CLI-01 | `outboxer` with no verb. | Unchanged relay behavior; no provisioning. |
| INIT-CLI-02 | `outboxer init` (no `--apply`). | Provisioning SQL is written to stdout; no database connection is opened; exit `0`. |
| INIT-CLI-03 | `outboxer init --apply`. | Connects with init credentials and executes the SQL; exit `0` on success. |
| INIT-CLI-04 | `init` with backend/AWS/health/stats flags set. | Those flags are accepted and ignored; provisioning proceeds. |
| INIT-CLI-05 | `init` run with no publishing backend enabled. | Succeeds; provisioning does not require a backend. |
| INIT-CLI-06 | `init` does not create Pub/Sub or SQS clients, health server, stats logger, or watchdog. | None are started; no cloud credentials are required. |
| INIT-CLI-07 | `outboxer unknown`. | Exits non-zero with an unknown-command error; the relay does not start. |
| INIT-CLI-08 | Relay or `init` invocation contains unexpected positional arguments (e.g. `outboxer typo`). | Exits non-zero and identifies the unexpected argument; the relay does not silently start. |

## Generated Outbox Table

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-TBL-01 | All optional column env vars non-empty (default). | Every column is generated regardless of backend/feature config: `id` (PK, identity), `payload` (`NOT NULL`), `target`, `destination`, `timestamp` (`timestamptz`), `options` (`jsonb`); table name from `EVENT_TABLE`. |
| INIT-TBL-02 | Optional columns. | `target`, `destination`, `timestamp`, `options` are all nullable; only `id` and `payload` are `NOT NULL`. |
| INIT-TBL-03 | An optional column is disabled (e.g. `EVENT_OPTIONS=disabled`). | That column is omitted entirely; no empty/invalid identifier is emitted; backend/feature flags do not otherwise affect column inclusion. |
| INIT-TBL-04 | Two configured non-empty columns resolve to the same name. | Config validation rejects it before any DDL; clear duplicate-column error. |
| INIT-TBL-05 | Custom `EVENT_*` column names and `EVENT_TABLE`. | Generated DDL uses the custom, safely-quoted identifiers for every column. |
| INIT-TBL-06 | Identifiers containing characters needing quoting. | All identifiers are quoted via `ident`; SQL is valid. |
| INIT-TBL-07 | Custom `PG_SCHEMA`. | Schema and table are emitted as two separately quoted identifiers; no object reference depends on `search_path`. |
| INIT-TBL-08 | Only the primary key index is generated. | No secondary indexes appear in the output. |

## Generated Schema

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-SCHEMA-01 | `PG_SCHEMA` unset. | `public` is used and explicitly qualifies every table/function reference. |
| INIT-SCHEMA-02 | `PG_SCHEMA=application`. | `CREATE SCHEMA IF NOT EXISTS application` is generated and all outbox, DLQ, notification, validation, and runtime SQL targets that schema. |
| INIT-SCHEMA-03 | Provisioning and runtime roles have different `search_path` values. | Both operate on the configured schema; no object is created or resolved through `search_path`. |
| INIT-SCHEMA-04 | `PG_SCHEMA` is empty. | Configuration validation fails before any DDL. |

## Generated DLQ Table

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-DLQ-01 | `DLQ_TABLE` unset (default). | No DLQ table is generated. |
| INIT-DLQ-02 | `DLQ_TABLE` set. | `CREATE TABLE IF NOT EXISTS <DLQ_TABLE>` with `id` (PK, identity) and `event jsonb NOT NULL`, matching the relay's DLQ shape check. |
| INIT-DLQ-03 | `DLQ_TABLE` equals `EVENT_TABLE`. | Rejected at validation, as in relay config. |

## Generated Notify Trigger

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-NTF-01 | `POLL_INTERVAL_MS == 0` (default). | The notify function and trigger are still generated; provisioning is independent of the runtime poll interval. |
| INIT-NTF-02 | Any poll interval. | A generic, schema-qualified `CREATE OR REPLACE FUNCTION` using `pg_notify(TG_ARGV[0], '')` plus `DROP TRIGGER IF EXISTS` + `CREATE TRIGGER ... AFTER INSERT ... FOR EACH STATEMENT` are generated. |
| INIT-NTF-03 | Custom `NOTIFY_CHANNEL`. | The channel is passed as the trigger argument (`TG_ARGV[0]`), safely quoted; the function body is not specialized per channel. |
| INIT-NTF-04 | Two outbox tables provisioned with different channels. | Each trigger passes its own channel argument; re-running for one table does not change the channel the other table's trigger notifies on. |

## Print Mode & Secret Safety

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-PRT-01 | Print mode with a run-role password configured. | The real `PG_PASSWORD` never appears in stdout; the run role is emitted as `CREATE ROLE ... LOGIN PASSWORD NULL` with a comment to configure auth separately. No known placeholder password is installed if applied verbatim. |
| INIT-PRT-02 | Print mode, no database reachable. | Generation succeeds with no connection attempt. |
| INIT-PRT-03 | Print output is fed back via `psql`/migration tooling. | The SQL is syntactically valid and applies cleanly to an empty database. |
| INIT-PRT-04 | Print output is stable for a fixed config. | Deterministic, golden-testable output. |

## Apply: Roles & Grants

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-ROLE-01 | `PG_INIT_USER` set, run role does not exist. | Run role is created `LOGIN` with `PG_PASSWORD`; granted `USAGE ON SCHEMA <PG_SCHEMA>`, `SELECT, DELETE` and `UPDATE (<EVENT_ID>)` on the qualified outbox table. |
| INIT-ROLE-06 | Run role processes a real event end to end. | The `UPDATE (<id>)` grant lets `SELECT ... FOR UPDATE` succeed; without it the batch select fails despite the `SELECT *` startup check passing. |
| INIT-ROLE-02 | `PG_INIT_USER` set, run role already exists. | Role is not recreated and its password is left untouched; grants are (re)applied idempotently. |
| INIT-ROLE-03 | `PG_INIT_USER` unset. | `--apply` connects as `PG_USER`/`PG_PASSWORD`; only schema objects are created; no role is created and no extra grants are issued. |
| INIT-ROLE-04 | `DLQ_TABLE` set, `PG_INIT_USER` set. | Run role is additionally granted `INSERT` on the DLQ table. |
| INIT-ROLE-05 | Run role's `PG_PASSWORD`, including quotes or `$$`, is interpolated into `CREATE ROLE`. | The literal is properly escaped (single quotes doubled), and a fresh random tagged dollar quote safely encloses the `DO` body; the password is never logged. |
| INIT-PROD-01 | `PG_PRODUCER_ROLES` lists one existing role. | That role is granted `USAGE` on `PG_SCHEMA` and `SELECT, INSERT` on the qualified outbox table; never `DELETE`. |
| INIT-PROD-02 | `PG_PRODUCER_ROLES` lists multiple comma-separated roles. | Each existing role receives the grants. |
| INIT-PROD-03 | A named producer role does not exist (`--apply`). | Apply fails with a clear message naming the missing role; transaction rolls back. |
| INIT-PROD-04 | `PG_PRODUCER_ROLES` includes the run role. | The run role additionally receives `SELECT, INSERT` (all-in-one setup). |
| INIT-PROD-05 | `PG_PRODUCER_ROLES` unset. | No producer grants are generated. |

## Idempotency & Safety

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-IDEM-01 | `init --apply` run twice against the same database. | Second run is a no-op; exit `0`; no error. |
| INIT-IDEM-02 | Outbox/DLQ table already exists. | `CREATE TABLE IF NOT EXISTS` is a no-op; the existing table is not altered. |
| INIT-IDEM-03 | Notify trigger already exists. | `DROP TRIGGER IF EXISTS` + `CREATE` replaces it cleanly; no error. |
| INIT-IDEM-04 | Existing table has a different shape than config implies (`--apply`). | Post-apply shape validation (same checks as relay startup) detects the mismatch, errors clearly, and rolls back the transaction; the existing table is not altered. |
| INIT-IDEM-05 | `init` issues no `DROP TABLE`/`ALTER TABLE`/`DELETE`/`TRUNCATE` on the outbox/DLQ tables or their data. | Verified across all generated SQL; only `DROP TRIGGER IF EXISTS` / `CREATE OR REPLACE FUNCTION` for the notify objects are present. |
| INIT-IDEM-06 | `--apply` succeeds, then the relay starts on the same config. | The relay validates without further DDL — a successful apply guarantees a relay-ready schema, never a deferred surprise. |

## Apply Atomicity

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-TXN-01 | A statement late in the apply fails (e.g. missing producer role). | The whole apply rolls back in one transaction; the database is unchanged. |
| INIT-TXN-02 | Init credentials lack required privileges. | Apply fails with a clear privilege error; nothing partial is committed. |

## Round-Trip Invariant (E2E)

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-E2E-01 | `init --apply` on an empty DB, then start the relay with the **same** config. | The relay passes `checkRequiredColumns` and the DLQ check and runs without further manual DDL. |
| INIT-E2E-02 | Provision with `POLL_INTERVAL_MS > 0`, insert an event as a producer role. | The trigger fires; the relay (run role) selects and delivers the event; the producer could not have deleted it. |
| INIT-E2E-03 | Provision with the run role, then connect as the run role and attempt `INSERT` on the outbox. | Denied — the run role has only `SELECT, DELETE` — confirming least privilege. |
| INIT-E2E-04 | Fresh ephemeral DB provisioned unattended via `init --apply` with init creds in env. | Database is relay-ready in one command; the automated test-DB use case works end to end. |
