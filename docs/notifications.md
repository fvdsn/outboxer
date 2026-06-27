# Low-Latency Notifications

By default Outboxer polls the outbox table continuously (`POLL_INTERVAL_MS=0`),
so latency is already near-immediate but the database is polled constantly. If
you raise `POLL_INTERVAL_MS` to reduce that polling load, events are then only
picked up on the next poll — up to one interval of latency.

A PostgreSQL `LISTEN`/`NOTIFY` trigger removes that trade-off: Outboxer waits for
a notification instead of sleeping out the full interval, so an insert wakes it
almost immediately while the interval load stays low. The poll interval becomes a
**safety sweep** rather than a latency floor.

This is an optional optimization. It is never required for correctness — a
missed or absent notification only delays an event until the next sweep, never
loses it.

## How it works

- `POLL_INTERVAL_MS=0` (the default): hot-loop polling. Outboxer never sleeps
  between empty batches, so there is nothing for a notification to interrupt and
  no listener is started. Installing the trigger has no effect.
- `POLL_INTERVAL_MS > 0`: between empty batches Outboxer waits for a notification
  on the configured channel, but wakes no later than `POLL_INTERVAL_MS` anyway.
  With the trigger installed, inserts wake it almost immediately; without it, it
  behaves exactly like plain polling at that interval.

There is no on/off switch for this feature — it is keyed entirely off
`POLL_INTERVAL_MS`.

## Installing the trigger

The trigger is provisioning DDL, run once by whoever owns the schema (typically
in the same migration that creates the outbox table). The running relay never
creates it; you install it yourself, or let [`outboxer init`](provisioning.md)
generate it when `POLL_INTERVAL_MS > 0`. The equivalent SQL is:

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

- The channel name (`outboxer_events`) is passed as the trigger argument and must
  match `NOTIFY_CHANNEL`.
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

With the trigger installed, raise `POLL_INTERVAL_MS` to whatever staleness you
are willing to tolerate as a backstop (for example a few seconds, or tens of
seconds), and let the trigger provide the low latency:

```sh
outboxer --poll-interval-ms=5000   # sweep at most every 5s; trigger wakes sooner
```

Remember `WATCHDOG_INTERVAL_MS` must remain at least 10x `POLL_INTERVAL_MS`.

## Caveats

- `LISTEN` does not work through a connection pooler in transaction-pooling mode
  (for example pgbouncer `pool_mode = transaction`). Outboxer's direct
  connection is unaffected; this only matters if you front Postgres with such a
  pooler.
- Outboxer still uses a single database connection: the listener borrows it only
  while idle and releases it before the next batch runs.
