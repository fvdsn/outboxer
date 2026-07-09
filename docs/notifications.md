# Low-Latency Notifications

Between empty batches Outboxer sleeps for `POLL_INTERVAL_MS` (default one
second), so plain polling alone would add up to one interval of latency. A
PostgreSQL `LISTEN`/`NOTIFY` trigger removes that trade-off: Outboxer waits for
a notification instead of sleeping out the full interval, so an insert wakes it
almost immediately while the polling load stays low. The poll interval is a
**safety sweep** rather than a latency floor.

The trigger is an optional optimization. It is never required for correctness —
a missed or absent notification only delays an event until the next sweep,
never loses it.

## How it works

- `POLL_INTERVAL_MS > 0` (default `1000`): between empty batches Outboxer waits
  for a notification on the configured channel, but wakes no later than
  `POLL_INTERVAL_MS` anyway. With the trigger installed, inserts wake it almost
  immediately; without it, it behaves exactly like plain polling at that
  interval.
- `POLL_INTERVAL_MS=0`: hot-loop polling. Outboxer never sleeps between empty
  batches, so there is nothing for a notification to interrupt and no listener
  is started. Installing the trigger has no effect. This trades constant
  database load for the lowest possible latency without the trigger.

There is no on/off switch for this feature — it is keyed entirely off
`POLL_INTERVAL_MS`.

## Installing the trigger

The trigger is provisioning DDL, run once by whoever owns the schema (typically
in the same migration that creates the outbox table). The running relay never
creates it; you install it yourself, or let [`outboxer init`](provisioning.md)
generate it (always, independent of `POLL_INTERVAL_MS`, so you can raise the
poll interval later without re-provisioning). The equivalent SQL is:

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

With the trigger installed, raise `POLL_INTERVAL_MS` to whatever staleness you
are willing to tolerate as a backstop (for example a few seconds, or tens of
seconds), and let the trigger provide the low latency:

```sh
outboxer --poll-interval-ms=5000   # sweep at most every 5s; trigger wakes sooner
```

The watchdog and health-staleness windows scale automatically to 10x the poll
interval when it exceeds their 10-minute and 5-minute floors.

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
