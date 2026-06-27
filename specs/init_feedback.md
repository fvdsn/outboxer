# Init / Provisioning Command Feedback

The proposed `init` command is well scoped for provisioning fresh databases.
In particular, the following design choices should be retained:

- `outboxer init` prints SQL without connecting to PostgreSQL.
- `outboxer init --apply` applies the same generated schema transactionally.
- `PG_INIT_USER` and `PG_INIT_PASSWORD` provide the provisioning identity.
- `PG_USER` and `PG_PASSWORD` continue to describe the relay's runtime identity.
- The running relay never performs DDL.
- Existing tables are not migrated or destructively reconciled.

The following issues should be addressed before implementation.

## Runtime role requires an UPDATE privilege

The relay's batch query uses `SELECT ... FOR UPDATE`. PostgreSQL requires the
role executing such a query to have `UPDATE` privilege on at least one column of
the selected table, in addition to `SELECT`.

Granting only `SELECT, DELETE` to the run role would therefore pass the initial
`SELECT *` database check but fail when the relay attempts to select an event.

The narrowest suitable grant is:

```sql
GRANT SELECT, DELETE ON events TO outboxer;
GRANT UPDATE (id) ON events TO outboxer;
```

The generated grant must use the configured event table and ID column names.
The E2E tests should insert and process a real event as the run role so that this
permission is exercised.

Reference:
<https://www.postgresql.org/docs/current/sql-select.html>

## Empty and duplicate optional column names

The relay allows some optional column names to be empty when their features are
not needed. In particular, an empty `EVENT_OPTIONS` disables event options.
Depending on the backend and age configuration, the target, destination, and
timestamp column names may also be empty.

Generating all optional columns unconditionally would produce an invalid empty
identifier for such configurations. The rule should instead be:

> Create every configured, non-empty optional column, regardless of whether the
> current backend configuration requires it.

This remains predictable while preserving the round-trip invariant for all
valid relay configurations.

Init validation should also reject configurations in which two configured
columns resolve to the same non-empty name, since they would produce duplicate
table columns.

## PostgreSQL schema must be defined

Resolved by the shared `PG_SCHEMA` setting, which defaults to `public`. Init
creates the configured schema when absent, and both provisioning and relay SQL
qualify schema objects explicitly. Table settings remain plain object names;
schema and table identifiers are never inferred by splitting dotted strings.

## Print-mode password placeholder must remain portable SQL

A `psql` variable such as `:'run_password'` is specific to `psql`, while a token
such as `:CHANGE_ME` is not valid generic SQL. Either choice conflicts with the
requirement that printed output can be consumed directly by different migration
tools.

A safe, portable representation is:

```sql
CREATE ROLE outboxer LOGIN PASSWORD NULL;
-- Configure authentication separately before starting Outboxer.
```

Apply mode can still use the real `PG_PASSWORD`. This keeps printed SQL valid,
prevents secret leakage, and avoids installing a known placeholder password if
the script is applied without modification.

## Idempotency wording

The command is idempotent, but a second invocation is not literally a no-op:
the notification function is replaced and the trigger is dropped and recreated.
Likewise, the implementation is not strictly additive because it issues
`DROP TRIGGER`.

The intended guarantee is better described as:

> Init is repeatable and non-destructive to tables and data. It may replace
> Outboxer-managed auxiliary objects such as its notification trigger.

The non-goals should prohibit dropping or altering tables and data rather than
prohibiting every `DROP` statement.

## Validate existing objects during apply

`CREATE TABLE IF NOT EXISTS` does not verify that an existing relation has the
expected shape. Deferring that error until the relay starts makes a successful
`init --apply` misleading.

After applying the generated DDL, init should validate the resulting event and
DLQ table shapes before committing. A mismatch should produce a clear error and
roll back the init transaction. Validation does not need to alter or reconcile
the existing objects.

## CLI rejection scenarios

The test plan should cover unknown verbs and unexpected positional arguments.
The current CLI parser can leave positional arguments unconsumed, so an input
such as `outboxer typo` must fail rather than accidentally start the relay.

Suggested scenarios:

| ID | Scenario | Expected |
| --- | --- | --- |
| INIT-CLI-07 | `outboxer unknown`. | Exits non-zero with an unknown-command error; the relay does not start. |
| INIT-CLI-08 | Relay or init invocation contains unexpected positional arguments. | Exits non-zero and identifies the unexpected argument. |

## Notification function reuse

Instead of embedding the configured channel directly in a function shared by
name, the trigger can pass the channel as an argument:

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
EXECUTE FUNCTION <PG_SCHEMA>.outboxer_notify('outboxer_events');
```

This keeps the function definition generic and prevents provisioning one outbox
table or channel from silently changing the channel used by another trigger.

## Producer grants

The proposed `SELECT, INSERT` producer grants are retained deliberately.
Although a narrower grant could expose less pending-event data, allowing
producers to inspect the outbox is useful operationally and favors operator
convenience. Producers still receive no `DELETE` privilege.
