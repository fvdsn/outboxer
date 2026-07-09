# Provisioning

The `outboxer init` command creates the database objects the relay needs — the
outbox table, the optional DLQ table, and the `LISTEN`/`NOTIFY` trigger — and,
optionally, the run role and grants for a least-privilege deployment. It
reuses the **same configuration as the relay**, so the objects it creates always
match what the relay later validates and uses.

This is the home for the DDL you would otherwise assemble by hand from
[Events](events.md), [Dead Letter Queue](dlq.md), and
[Notifications](notifications.md). The running relay never issues DDL; `init` is a
separate command, run once by a privileged role at database provisioning time.

## Print or apply

```sh
# Print the SQL to stdout (no database connection). Feed it into your migrations.
outboxer init

# Connect and execute the SQL in a single transaction.
outboxer init --apply
```

Print mode is the default. It connects to nothing and emits portable SQL you can
review, commit, and run through your own migration tool (Flyway, Liquibase,
Atlas, a migration PR). Apply mode runs the same SQL transactionally and
validates the resulting schema before committing, so a successful apply leaves a
relay-ready database.

## What it creates

Everything is derived from the relay configuration — there are no
provisioning-specific schema flags:

- **Outbox table** (`EVENT_TABLE`) with each configured column (`EVENT_ID`,
  `EVENT_PAYLOAD`, and the optional `EVENT_TARGET`, `EVENT_DESTINATION`,
  `EVENT_TIMESTAMP`, `EVENT_OPTIONS`). An optional column set to `disabled` is
  omitted. Only `id` and `payload` are `NOT NULL`; the rest are nullable.
  The table also gets eager per-table autovacuum settings (small scale
  factors with absolute thresholds): the outbox churns every row — one
  insert and one delete each — so the PostgreSQL defaults defer vacuum and
  analyze exactly when the table is busiest, accumulating bloat and stale
  planner statistics. After a **bulk backfill**, run `ANALYZE <table>`
  manually rather than waiting for autoanalyze.
- **DLQ table** (`DLQ_TABLE`), only when set to a table name.
- **Notify function and trigger**, always, on the `outboxer_<table>` channel,
  independent of the relay's `POLL_INTERVAL_MS`, which keys the `LISTEN`/`NOTIFY`
  wake-ups. See [Notifications](notifications.md).
- **PostgreSQL schema** (`PG_SCHEMA`), created when absent. It defaults to
  `public`.

Objects are created with `CREATE TABLE IF NOT EXISTS` / `CREATE OR REPLACE
FUNCTION`, so `init` is repeatable and never drops, alters, or deletes a table or
its data. (It does recreate its own notification trigger on each run.)

Every object reference is explicitly qualified with `PG_SCHEMA`, so provisioning
and relay execution do not depend on either database role's `search_path`.
`EVENT_TABLE` and `DLQ_TABLE` remain plain object names; configure the schema
separately rather than putting a dotted name in either setting.

## Roles

`init` distinguishes three roles:

| Role | Configured by | Purpose |
| --- | --- | --- |
| Init / admin | `PG_INIT_USER` / `PG_INIT_PASSWORD` | The identity `--apply` connects as. Needs `CREATE` on the database for schema provisioning, DDL rights in an existing target schema, and, to manage the run role, `CREATEROLE` (managed Postgres admin accounts have it). |
| Run | `PG_USER` / `PG_PASSWORD` | The relay's runtime identity, in both verbs. |
| Producer | `PG_PRODUCER_ROLES` | The application(s) that insert events. |

**The presence of `PG_INIT_USER` is the single knob** for role management:

- **Set:** `init` connects as the provisioning role and additionally creates the
  run role (if missing) and applies all grants. The run role gets `USAGE` on
  `PG_SCHEMA`, `SELECT, DELETE` and `UPDATE (id)` on the outbox table (the
  `UPDATE (id)` is required by `SELECT ... FOR UPDATE`), and `INSERT` on the DLQ
  table when configured. Each role in `PG_PRODUCER_ROLES` is granted `USAGE` on
  `PG_SCHEMA` and `SELECT, INSERT` on the outbox table.
- **Unset:** `init` connects as `PG_USER` / `PG_PASSWORD` and only creates the
  schema objects, assuming that user already holds the necessary DDL rights. No
  role is created and no grants are issued. This is the simple single-user case.

The run role's password is taken from `PG_PASSWORD`. In **print mode** the run
role is emitted as `CREATE ROLE ... LOGIN PASSWORD NULL` with a reminder to
configure authentication separately, so no secret is written to stdout and no
usable placeholder password is installed. Existing roles are never modified — a
re-run leaves an already-present role's password untouched.

Producer roles are **grant-only**: they must already exist (`init` errors if a
named role is missing) and `init` never creates them or sets their passwords,
since the producer is your application's own database user.

## Example: automated test database

A fresh, locked-down database from a single command, driven by the same
environment that runs the relay:

```sh
export EVENT_TABLE=events
export DLQ_TABLE=dead_letters
export POLL_INTERVAL_MS=5000
export PG_HOST=... PG_DATABASE=app
export PG_SCHEMA=public                              # or an application schema
export PG_INIT_USER=admin PG_INIT_PASSWORD=...   # provisioning identity
export PG_USER=relay PG_PASSWORD=...             # run role init will create
export PG_PRODUCER_ROLES=app_service             # existing producer role

outboxer init --apply
```

The orchestrator creates the empty database with an admin, `init --apply`
provisions the schema and the locked-down run role, and the relay then runs with
`PG_USER` / `PG_PASSWORD` from the same configuration.
