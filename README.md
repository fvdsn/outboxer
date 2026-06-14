# Outboxer

Outboxer is a small worker for the transactional outbox pattern. It reads events
from a PostgreSQL table, publishes them to Google Pub/Sub or AWS SQS, and deletes
rows that were successfully published.

It is meant to run as a long-lived container with a health endpoint.

## Delivery Semantics

Outboxer provides at-least-once delivery. A message may be published more than
once if the process stops after publishing but before the database transaction is
committed, or if a queue provider accepts a message but a later operation fails.

Consumers should be idempotent.

Outboxer holds a database transaction while publishing a batch. This keeps the
delete behavior simple and preserves ordering behavior, but it also means slow
queue calls can hold row locks for longer.

## Event Table

The default table and column names are:

```sql
CREATE TABLE events (
    id text PRIMARY KEY,
    timestamp timestamptz,
    payload text NOT NULL,
    target text,
    destination text,
    ordering_key text,
    attributes jsonb
);
```

Only `id` and `payload` are strictly required by Outboxer. `destination`
defaults to `DEFAULT_TOPIC` for Pub/Sub events. For SQS events, `destination`
must contain the SQS queue URL.

### Pub/Sub Example

```sql
INSERT INTO events (id, timestamp, payload, destination, ordering_key, attributes)
VALUES (
    'event-1',
    now(),
    '{"type":"user.created","id":"123"}',
    'user-events',
    'user-123',
    '{"source":"users"}'
);
```

### SQS Example

```sql
INSERT INTO events (id, timestamp, payload, target, destination, attributes)
VALUES (
    'event-2',
    now(),
    '{"type":"invoice.created","id":"456"}',
    'sqs',
    'https://sqs.eu-west-1.amazonaws.com/123456789012/invoices',
    '{"source":"billing"}'
);
```

Attributes must be strings. Non-string attributes are dropped and logged.

## Configuration

Outboxer reads environment variables and also loads a local `.env` file when
present. Every configuration value can also be set with a CLI flag.

Configuration precedence is:

1. CLI flags
2. environment variables or `.env`
3. defaults

CLI flags use kebab-case names:

```sh
outboxer \
    --pg-host=localhost \
    --pg-user=postgres \
    --event-table=events \
    --default-topic=user-events
```

Use `--help` to list every flag. The help output includes the associated
environment variable and default value:

```sh
outboxer --help
```

| Variable | Default | Description |
| --- | --- | --- |
| `EVENT_TABLE` | `events` | Outbox table name. |
| `EVENT_ID` | `id` | Event id column. |
| `EVENT_TIMESTAMP` | `timestamp` | Event timestamp column, used for latency logs. |
| `EVENT_PAYLOAD` | `payload` | Event payload column. |
| `EVENT_TARGET` | `target` | Target column. Use `sqs` for SQS, anything else for Pub/Sub. |
| `EVENT_DESTINATION` | `destination` | Pub/Sub topic name or SQS queue URL. |
| `EVENT_ORDERING_KEY` | `ordering_key` | Ordering key / FIFO message group column. |
| `EVENT_ATTRIBUTES` | `attributes` | JSON attributes column. |
| `DEFAULT_TOPIC` | `default` | Pub/Sub topic used when an event has no topic. |
| `BATCH_SIZE` | `32` | Maximum rows selected per batch. |
| `BATCH_WORKERS` | `8` | Number of parallel publisher workers per batch. |
| `BATCH_MAX_SEQUENTIAL` | `8` | Maximum ordered events assigned to one worker in a batch. |
| `ERROR_COOLDOWN_MS` | `5000` | Sleep after batch or database errors. |
| `POLL_INTERVAL_MS` | `0` | Sleep after an empty batch. The default keeps polling immediately. |
| `WATCHDOG_INTERVAL_MS` | `600000` | Watchdog interval. |
| `HEALTH_PORT` | `PORT` or `0` | HTTP health server port. `0` disables the server. |
| `PG_HOST` | `localhost` | PostgreSQL host. |
| `PG_PORT` | `5432` | PostgreSQL port. |
| `PG_USER` | `postgres` | PostgreSQL user. |
| `PG_PASSWORD` | empty | PostgreSQL password. |
| `PG_DATABASE` | `postgres` | PostgreSQL database. |
| `PG_SSL` | `false` | Enable PostgreSQL TLS. |
| `PG_SSL_REJECT_UNAUTHORIZED` | `false` | Verify PostgreSQL TLS certificates. |
| `PG_CONNECT_TIMEOUT_MS` | `10000` | PostgreSQL connect timeout in milliseconds. |
| `PG_MAX_CONNECTIONS` | `10` | PostgreSQL max open connections. |
| `PUBSUB_API_ENDPOINT` | empty | Optional Pub/Sub API endpoint override. |
| `AWS_REGION` | empty | AWS region for SQS and STS. |
| `AWS_ROLE_ARN` | empty | Optional AWS role to assume before publishing to SQS. |
| `AWS_ROLE_SESSION_NAME` | `outboxer` | AWS assume-role session name. |
| `AWS_ROLE_DURATION_SECONDS` | `3600` | AWS assumed-role duration. |
| `AWS_CREDENTIAL_REFRESH_WINDOW_MS` | `300000` | Refresh assumed credentials before expiry. |

## Authentication

Google Pub/Sub uses Application Default Credentials.

AWS SQS uses the AWS SDK default credential chain. If `AWS_ROLE_ARN` is set,
Outboxer assumes that role before publishing to SQS.

## Health Endpoint

The HTTP server starts only when `HEALTH_PORT`, `PORT`, or
`--health-port` is set to a positive port. It returns `200 all good` for
health checks. Successful health checks are logged at debug level. `DELETE`
requests ask the process to shut down gracefully and are logged at info level.

## Layout

```text
cmd/outboxer/       executable entrypoint
internal/outboxer/  service implementation
```

## Run

```sh
just run
```

## Build

```sh
just build
```

To build a local binary:

```sh
just binary
```

## Test

```sh
just test
```

The default tests mock Pub/Sub and SQS through narrow publisher interfaces.

## Integration Test

The integration test uses a real PostgreSQL database from Docker Compose:

```sh
just integration
```

This starts Postgres on `localhost:54329` and runs all tests with:

```sh
OUTBOXER_INTEGRATION_PG_DSN='postgres://outboxer:outboxer@localhost:54329/outboxer?sslmode=disable' go test ./... -count=1
```

To stop and remove the test database:

```sh
just db-down
```

Useful commands are listed with:

```sh
just
```
