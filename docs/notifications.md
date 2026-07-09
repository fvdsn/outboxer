# Low-Latency Notifications

Between empty batches Outboxer waits for a PostgreSQL `LISTEN`/`NOTIFY`
wake-up on a dedicated, permanently subscribed connection, bounded by a
one-second safety sweep. An insert wakes the relay almost immediately (the
July 2026 cloud benchmark measured a 55 ms median, 85 ms p99 commit-to-consumer
on an idle relay); the sweep only matters when the listener connection is
re-establishing after a failure, so it is a durability net rather than a
latency floor.

The trigger is never required for correctness — a missed or absent
notification only delays an event until the next sweep, never loses it.
Without the trigger installed, the relay behaves like a plain one-second
poller.

## Installing the trigger

The trigger is provisioning DDL, run once by whoever owns the schema (typically
in the same migration that creates the outbox table). The running relay never
creates it; you install it yourself, or let [`outboxer init`](provisioning.md)
generate it. The equivalent SQL is:

```sql
CREATE OR REPLACE FUNCTION public.outboxer_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(TG_ARGV[0], '');
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outboxer_notify
AFTER INSERT ON public.events
FOR EACH STATEMENT
EXECUTE FUNCTION public.outboxer_notify('outboxer_events');
```

- The channel name is passed as the trigger argument and is derived from the
  event table as `outboxer_<table>` (`outboxer_events` for the default table),
  identically by `init` and the relay, so there is nothing to coordinate.
- Replace `public` with the configured `PG_SCHEMA` when using a custom schema.
- The function is **generic**: it reads the channel from the trigger's argument
  (`TG_ARGV[0]`) via `pg_notify` rather than hardcoding it in the body. A
  PostgreSQL trigger function is a schema-level object shared by name across every
  trigger that references it, so baking the channel into the body would mean
  re-creating the function for a second outbox table silently repoints the first
  table's channel too. Keeping the function channel-agnostic and supplying the
  channel per trigger avoids that and lets one `outboxer_notify()` serve many
  outbox tables.
- `FOR EACH STATEMENT` means a bulk insert of many rows fires a single
  notification. The notification carries no payload; it only means "go look".
- `pg_notify` is transactional (identical to `NOTIFY` in that respect), delivered
  on `COMMIT`, so Outboxer is woken only once the inserted rows are visible.

Outboxer's runtime database role needs **no extra privileges** for this:
`LISTEN` and `NOTIFY` require none. The trigger only adds privileges at install
time (for the migration role), not to the running relay.

## Recommended usage

Install the trigger (or let `init` do it) and do nothing else — the wake-up
path needs no tuning.

## Caveats

- `LISTEN` does not work through a connection pooler in transaction-pooling mode
  (for example pgbouncer `pool_mode = transaction`). Outboxer's direct
  connection is unaffected; this only matters if you front Postgres with such a
  pooler.
- Outboxer uses exactly two database connections: one for batches and one held
  by the notification listener. The subscription is persistent because Postgres
  delivers a `NOTIFY` only to sessions listening at commit time — an event
  committed while a batch runs buffers its wake-up on the listener connection
  instead of waiting out the poll backstop.
