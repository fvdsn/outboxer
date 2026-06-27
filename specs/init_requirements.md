# Init / Provisioning Command Requirements

Status: implementation requirements.

Outboxer should provide an `init` subcommand that provisions the database
objects the relay needs — the outbox table, the optional DLQ table, the optional
`LISTEN`/`NOTIFY` trigger — and, optionally, the roles and grants for a
least-privilege deployment. It reuses the **same configuration** as the relay, so
the objects it creates always match what the relay later validates and uses.

The motivating use case is **automated provisioning of fresh databases**, such as
spinning up many ephemeral databases for test environments and scenarios. A
single `outboxer init --apply` against an empty database, driven by the same
`.env` that runs the relay, should leave the database ready for the relay to
start with a locked-down runtime role.

This does not change Outboxer's long-standing "validate, do not create schema"
stance for the *running relay*. The relay still never issues DDL. `init` is a
separate, explicitly-invoked command run by a provisioning role — typically once,
at database creation — and is the sanctioned home for the DDL that operators
previously had to assemble by hand from [`events.md`](../docs/events.md),
[`dlq.md`](../docs/dlq.md), and [`notifications.md`](../docs/notifications.md).

## Design Principle: Same Config, Derived Schema

`init` takes **no schema arguments of its own**. Everything it creates is derived
from the existing relay configuration (`PG_SCHEMA`, `EVENT_TABLE`, the `EVENT_*`
column names, `DLQ_TABLE`, `NOTIFY_CHANNEL`, `POLL_INTERVAL_MS`, the backend
enable flags, the `PG_*` connection settings). The same environment that runs
the relay describes what `init` provisions.

The binding invariant:

> The schema produced by `init` for a given configuration, when the relay is
> started with that **same** configuration, passes the relay's own startup
> validation (`checkRequiredColumns`, the DLQ shape check) without further
> manual DDL.

This makes correctness testable as a round-trip: provision with a config, then
boot the relay with the same config and confirm it validates and runs.

## Subcommand Dispatch

Today `Run` goes straight to `loadConfig` and starts the relay. `init` introduces
the first verb:

- `outboxer init` — emit the provisioning SQL to stdout (default; see
  [Print vs Apply](#print-vs-apply)).
- `outboxer init --apply` — connect and execute the provisioning SQL.
- No verb (`outboxer [flags]`) — unchanged relay behavior. This preserves backward
  compatibility for existing invocations.
- `init` accepts the same flags/env as the relay; flags unrelated to provisioning
  (backend endpoints, AWS, health port, stats) are accepted and ignored rather
  than rejected, so a single `.env` works for both verbs.
- **Unknown verbs and stray positional arguments must fail loudly.** The current
  parser stops at the first non-flag token and leaves leftovers unconsumed, so
  `outboxer typo` would silently start the relay. Both the relay and `init` paths
  must reject an unexpected positional argument (and an unknown verb) with a
  non-zero exit and a clear message, rather than ignoring it.

`init` is **database-only**. It must not create Pub/Sub or SQS clients, open the
health server, start the stats logger, or the watchdog. It needs no cloud
credentials.

## Print vs Apply

Two modes, sharing one SQL generator:

- **Print (default).** Emits the full provisioning SQL to stdout and exits.
  Requires **no database connection** and no DDL privileges — it is pure
  generation from config. This is the primary mode: it lets operators feed the
  SQL into their own migration tooling (Flyway, Liquibase, Atlas, a reviewed
  migration PR) rather than letting the application mutate schema, and it makes
  the generator unit-testable as golden output with no database.
- **Apply (`--apply`).** Connects using the init credentials (see
  [Connection & Roles](#connection--roles)) and executes the same SQL inside a
  **single transaction**, so a failure leaves the database unchanged. Postgres
  runs `CREATE TABLE` / `CREATE ROLE` / `GRANT` transactionally, so atomic apply
  is achievable.

The generated SQL is identical between modes except for secret handling (below).

## Secrets Must Not Leak in Print Mode

`init` may emit `CREATE ROLE ... LOGIN PASSWORD '...'` for the run role. Print
mode must **never** write a real password to stdout: the printed SQL is expected
to be committed to repositories, captured in CI logs, and pasted into migration
files.

- In **print** mode, the run role is created with **`PASSWORD NULL`** and a
  comment directing the operator to configure authentication separately:

  ```sql
  CREATE ROLE outboxer LOGIN PASSWORD NULL;
  -- Configure authentication separately before starting Outboxer.
  ```

  This is portable, valid SQL across migration tools (unlike a `psql`-specific
  `:'var'` or a non-SQL `:CHANGE_ME` token), leaks no secret, and — crucially —
  installs **no** known placeholder password if the script is applied verbatim.
- In **apply** mode, the real `PG_PASSWORD` is used. Because DDL cannot be
  bind-parameterized, the value is interpolated as a properly-escaped SQL string
  literal (single quotes doubled), reusing the existing quoting helpers
  (`ident`, `sqlStringLiteral`). The surrounding `DO` body uses a fresh random
  tagged dollar quote so dollar sequences in the password cannot terminate it.
  The password is never logged.

## Generated Objects

What `init` generates is driven entirely by config:

### Schema

- `CREATE SCHEMA IF NOT EXISTS <PG_SCHEMA>`, defaulting to `public`.
- Every schema object reference in both init and relay mode is explicitly
  qualified with `PG_SCHEMA`; behavior must not depend on either connection's
  `search_path`.
- `EVENT_TABLE` and `DLQ_TABLE` are object names, not dotted qualified names.
  The schema is configured separately.

### Outbox table

- `CREATE TABLE IF NOT EXISTS <EVENT_TABLE>` with every **configured (non-empty)**
  column, using the configured column names. The rule is:

  > Create every configured, non-empty optional column, regardless of whether the
  > current backend/feature configuration requires it.

  This keeps the schema predictable — column inclusion no longer depends on
  backend flags or `MAX_EVENT_AGE_MS` — while still honoring the relay's
  convention that an **empty** optional column env var (e.g. `EVENT_OPTIONS=""`)
  disables that column entirely. An empty name must never be emitted as an empty
  identifier.
  - `<EVENT_ID>` `bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY` — always
    (`EVENT_ID` is required and non-empty).
  - `<EVENT_PAYLOAD>` `text NOT NULL` — always (`EVENT_PAYLOAD` is required).
  - `<EVENT_TARGET>` `text` (nullable) — when `EVENT_TARGET` is non-empty.
  - `<EVENT_DESTINATION>` `text` (nullable) — when `EVENT_DESTINATION` is non-empty.
  - `<EVENT_TIMESTAMP>` `timestamptz` (nullable) — when `EVENT_TIMESTAMP` is non-empty.
  - `<EVENT_OPTIONS>` `jsonb` (nullable) — when `EVENT_OPTIONS` is non-empty.
  - Only `id` and `payload` are `NOT NULL`; the rest are nullable so the relay's
    "optional columns may be absent / empty" semantics still hold.
- The primary key on `<EVENT_ID>` is the only index `init` creates. The batch
  select orders by the id column, which the primary key index already covers, so
  no secondary indexes are generated. (Destination/target sharding indexes are a
  possible future addition tied to the multi-instance work, not part of this
  feature.)

### DLQ table

- Only when `DLQ_TABLE` is set. `CREATE TABLE IF NOT EXISTS <DLQ_TABLE>` with
  `id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY` and `event jsonb NOT
  NULL`, matching the relay's DLQ shape check.

### Notify trigger

- Keyed off `POLL_INTERVAL_MS`, consistent with the notify feature's "no modes"
  principle:
  - `POLL_INTERVAL_MS > 0`: generate a **generic** `CREATE OR REPLACE FUNCTION`
    and a `DROP TRIGGER IF EXISTS ... ; CREATE TRIGGER ... AFTER INSERT ON
    <EVENT_TABLE> FOR EACH STATEMENT` that passes the channel as a trigger
    argument rather than baking it into the function body:

    ```sql
    CREATE OR REPLACE FUNCTION <PG_SCHEMA>.outboxer_notify() RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify(TG_ARGV[0], '');
      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER outboxer_notify
    AFTER INSERT ON <PG_SCHEMA>.<EVENT_TABLE>
    FOR EACH STATEMENT
    EXECUTE FUNCTION <PG_SCHEMA>.outboxer_notify('<NOTIFY_CHANNEL>');
    ```

    Passing the channel via `TG_ARGV[0]` keeps the shared-by-name function
    generic, so provisioning one outbox table or channel cannot silently change
    the channel another table's trigger notifies on. (This supersedes the
    channel-in-body form shown in [`notifications.md`](../docs/notifications.md);
    those docs should be aligned to this form.)
  - `POLL_INTERVAL_MS == 0` (default): the relay never `LISTEN`s, so the trigger
    would have no effect; `init` does not generate it.

## Connection & Roles

Three roles, with `init` owning the lifecycle of the run role and only granting to
producers.

### Init connection

- `PG_INIT_USER` / `PG_INIT_PASSWORD` — the credential `--apply` connects as. Must
  have `CREATE` on the database for schema provisioning, DDL rights in an
  existing target schema, and, when it manages the run role, `CREATEROLE`
  (managed Postgres admin accounts have `CREATEROLE` without being superuser, so
  this works on RDS / Cloud SQL / etc.).
- **Presence of `PG_INIT_USER` is the single knob** for role management:
  - Set: `--apply` connects as the init role and additionally creates the run
    role (if missing) and applies all grants.
  - Unset: `--apply` connects as `PG_USER` / `PG_PASSWORD` and only creates the
    schema objects, assuming that user already holds the necessary DDL rights.
    No role is created and no grants beyond what that user can self-issue. This is
    the simple single-user dev case.

### Run role (the relay's runtime identity)

- Described by `PG_USER` / `PG_PASSWORD` in **both** verbs: in relay mode it is the
  identity the relay connects as; in `init` mode it is the role `init` creates and
  grants to. The same `.env` therefore describes the run user in both cases.
- Created with `LOGIN` and `PG_PASSWORD` **only if it does not already exist**.
  Existing roles are left untouched — `init` never `ALTER`s an existing role's
  password (least surprise; rotation is out of scope). `CREATE ROLE` has no
  `IF NOT EXISTS`, so creation is guarded by a randomly tagged
  `DO $outboxer_…$ ... IF NOT EXISTS (SELECT FROM pg_roles ...)
  ... $outboxer_…$` block.
- Granted the minimal runtime privileges:
  - `USAGE ON SCHEMA <PG_SCHEMA>`.
  - `SELECT, DELETE` on `<EVENT_TABLE>` — the relay selects rows and deletes
    confirmed ones; it never inserts events.
  - `UPDATE (<EVENT_ID>)` on `<EVENT_TABLE>` — the batch query is `SELECT ... FOR
    UPDATE`, and PostgreSQL requires `UPDATE` privilege on at least one column of
    a row-locked table in addition to `SELECT`. Column-level `UPDATE` on just the
    id column is the narrowest grant that satisfies this; the relay never writes
    column values. Without it, startup's `SELECT *` check passes but the first
    batch select fails. (Ref: PostgreSQL `SELECT ... FOR UPDATE` docs.)
  - `INSERT` on `<DLQ_TABLE>` — only when `DLQ_TABLE` is set.
  - No `LISTEN`/`NOTIFY` privilege is needed; those require none.
  - With `GENERATED BY DEFAULT AS IDENTITY`, table-level privileges suffice; no
    separate sequence grant is required.

### Producer roles (the application that inserts events)

- `PG_PRODUCER_ROLES` — a **comma-separated list** of existing role names.
- **Grant-only.** For each named role, `init` issues `GRANT SELECT, INSERT ON
  <PG_SCHEMA>.<EVENT_TABLE>` and `GRANT USAGE ON SCHEMA <PG_SCHEMA>`. `init`
  never creates a producer role and never sets its password: the producer is the
  application's own database user with its own credential lifecycle and
  privileges that Outboxer has no reason to manage.
- A named producer role that does not exist is an error in `--apply` (clear
  message naming the role); print mode emits the grant regardless, since print is
  offline.
- Producers receive `SELECT` and `INSERT` on the outbox table: `INSERT` to enqueue
  events, `SELECT` so a producer can read back rows it enqueued (for example to
  confirm or inspect pending events). They are never granted `DELETE` — removing
  sent rows is the relay's job alone.
- For all-in-one test setups, an operator may list the run role in
  `PG_PRODUCER_ROLES` so it also receives `INSERT`, or simply let the producer use
  the init/admin user.

## Idempotency & Safety

The intended guarantee is:

> `init` is **repeatable** and **non-destructive to tables and data**. It may
> replace Outboxer-managed auxiliary objects (its notification function and
> trigger), so a re-run is not literally a no-op, but it never drops, alters, or
> deletes the outbox/DLQ tables or any row in them.

- Tables: `CREATE TABLE IF NOT EXISTS`. Primary key: created with the table.
- Function: `CREATE OR REPLACE FUNCTION` (replaced on every run).
- Trigger: `DROP TRIGGER IF EXISTS` then `CREATE TRIGGER` (dropped and recreated
  on every run — this is the reason a re-run is repeatable rather than a strict
  no-op).
- Run role: `DO`-block guarded `CREATE ROLE` (create-if-missing; existing
  password untouched).
- Grants: inherently idempotent.

`init` never issues `DROP TABLE`/`ALTER TABLE`/`DELETE`/`TRUNCATE` against the
outbox or DLQ tables, and never alters their data. It is a provisioner, not a
migration engine: it does not reconcile schema drift on an existing table.

## Validating the Result

`CREATE TABLE IF NOT EXISTS` does not check that a pre-existing relation has the
expected shape, so a successful `--apply` could otherwise be misleading. To keep
the [round-trip invariant](#design-principle-same-config-derived-schema) honest:

- After executing the generated DDL and **before committing**, `--apply` runs the
  same shape validation the relay performs at startup (`checkRequiredColumns` for
  the outbox table, the DLQ shape check) against the resulting tables.
- A mismatch produces a clear error and **rolls back** the init transaction.
  Validation only reads catalog/shape; it never alters or reconciles existing
  objects.
- Config-level validation runs first (before any DDL): in addition to the relay's
  applicable checks, `init` **rejects a configuration in which two configured,
  non-empty column names resolve to the same identifier**, since that would
  generate duplicate table columns.

## Validation

`init` validates only the configuration subset it needs to generate the schema
(schema/table/column names, DLQ name, notify channel, backend flags that decide
whether the target column is required, and — for `--apply` — the connection
settings). It does not require a publishing backend to be enabled, since
provisioning is independent of where events will later be sent.

## Non-Goals

- No schema migration / drift reconciliation; `init` only creates missing objects.
- No dropping, altering, truncating, or deleting of the outbox/DLQ **tables or
  their data**. (Re-creating Outboxer's own notification trigger/function via
  `DROP TRIGGER IF EXISTS` / `CREATE OR REPLACE` is allowed and expected.)
- No producer-role creation or password management; producers are grant-only.
- No run-role password rotation; existing roles are left untouched.
- No secondary indexes beyond the primary key.
- No automatic trigger creation by the running relay (unchanged).
- The relay continues to issue no DDL; provisioning is exclusively `init`'s job.

## Acceptance Scenarios

The test plan lives in [`init_scenarios.md`](init_scenarios.md).
