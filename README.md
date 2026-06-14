# Outboxer

[![CI](https://github.com/fvdsn/outboxer/actions/workflows/ci.yml/badge.svg)](https://github.com/fvdsn/outboxer/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/fvdsn/outboxer)](https://goreportcard.com/report/github.com/fvdsn/outboxer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Outboxer is a small worker for the transactional outbox pattern. It reads events
from a PostgreSQL table, publishes them to Google Pub/Sub or AWS SQS, and deletes
rows that were successfully published.

It is meant to run as a long-lived container with a health endpoint.

## Backends

Both backends are opt-in. Enable the ones you need with `PUBSUB_ENABLED=true`
and/or `SQS_ENABLED=true`. At least one must be enabled or Outboxer exits at
startup. A backend's client is only created when it is enabled, so disabled
backends never need credentials or configuration.

Routing depends on how many backends are enabled:

- **One backend enabled:** the `target` column is optional and every event is
  sent to that backend.
- **Both backends enabled:** each event's `target` column must be `pubsub` or
  `sqs`. A configured `target` column (`EVENT_TARGET`) is required, otherwise
  Outboxer exits at startup.

Events whose target cannot be routed to an enabled backend (an unknown value, or
an empty target when both backends are enabled) are logged as errors and left in
the table. Fix the row or the configuration and they will be picked up again.

## Delivery Semantics

Outboxer provides at-least-once delivery. A message may be published more than
once if the process stops after publishing but before the database transaction is
committed, or if a queue provider accepts a message but a later operation fails.

Consumers should be idempotent.

Outboxer holds a database transaction while publishing a batch. This keeps the
delete behavior simple and preserves ordering behavior, but it also means slow
queue calls can hold row locks for longer. Every database query and publish call
is bounded by a timeout (`PG_QUERY_TIMEOUT_MS`, `PUBLISH_TIMEOUT_MS`) so a single
hung call cannot stall the batch indefinitely, and the watchdog remains as a
last-resort backstop.

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

Outboxer reads the table with `SELECT *` and maps columns by name, so optional
columns may simply be left out of the table. Required columns are checked at
startup and a missing one stops the process with a clear error.

| Column | Required when | Notes |
| --- | --- | --- |
| `id` | always | Primary key. Used to order and delete rows. |
| `payload` | always | Message body. |
| `target` | both backends enabled | Backend selector: `pubsub` or `sqs`. See [Backends](#backends). |
| `destination` | a backend is enabled without a default destination | Pub/Sub topic name or SQS queue URL. Optional when `DEFAULT_PUBSUB_TOPIC` / `DEFAULT_SQS_QUEUE_URL` covers it. |
| `timestamp` | never | Used only for latency logging. |
| `ordering_key` | never | Enables ordered / FIFO delivery. |
| `attributes` | never | JSON object of string message attributes. |

A minimal single-backend table can therefore be just `id` and `payload`, with a
default destination configured.

### Pub/Sub Example

```sql
INSERT INTO events (id, timestamp, payload, target, destination, ordering_key, attributes)
VALUES (
    'event-1',
    now(),
    '{"type":"user.created","id":"123"}',
    'pubsub',
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

### SQS FIFO Queues

Outboxer detects FIFO queues from the `.fifo` suffix on the queue URL, so
standard and FIFO queues are handled correctly without extra configuration:

- **FIFO queues** receive a `MessageGroupId` (the event's `ordering_key`, or a
  random group when it has none) and a `MessageDeduplicationId` set to the event
  `id`. Using the event id for deduplication means re-sends after a crash are
  deduplicated by SQS, so content-based deduplication is not required on the
  queue.
- **Standard queues** never receive a `MessageGroupId` or
  `MessageDeduplicationId`. An `ordering_key` on a standard-queue event is
  ignored by SQS.

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
    --pubsub-enabled \
    --default-pubsub-topic=user-events
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
| `EVENT_TARGET` | `target` | Backend selector column. Values `pubsub` or `sqs`. Required only when both backends are enabled. |
| `EVENT_DESTINATION` | `destination` | Pub/Sub topic name or SQS queue URL column. |
| `EVENT_ORDERING_KEY` | `ordering_key` | Ordering key / FIFO message group column. |
| `EVENT_ATTRIBUTES` | `attributes` | JSON attributes column. |
| `PUBSUB_ENABLED` | `false` | Enable publishing to Google Pub/Sub. |
| `SQS_ENABLED` | `false` | Enable publishing to AWS SQS. |
| `DEFAULT_PUBSUB_TOPIC` | `default` | Pub/Sub topic used when an event has no destination. |
| `DEFAULT_SQS_QUEUE_URL` | empty | SQS queue URL used when an event has no destination. |
| `PUBSUB_PROJECT_ID` | empty | Google Cloud project for Pub/Sub. Detected from ADC when empty. |
| `BATCH_SIZE` | `32` | Maximum rows selected per batch. |
| `BATCH_WORKERS` | `8` | Number of parallel publisher workers per batch. |
| `BATCH_MAX_SEQUENTIAL` | `8` | Maximum ordered events assigned to one worker in a batch. |
| `ERROR_COOLDOWN_MS` | `5000` | Sleep after batch or database errors. |
| `POLL_INTERVAL_MS` | `0` | Sleep after an empty batch. The default keeps polling immediately. |
| `WATCHDOG_INTERVAL_MS` | `600000` | Watchdog interval. Must be at least 10x `POLL_INTERVAL_MS` when polling is enabled. |
| `PUBLISH_TIMEOUT_MS` | `30000` | Timeout for a single publish call. `0` disables it. |
| `HEALTH_PORT` | `PORT` or `0` | HTTP health server port. `0` disables the server. |
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error`. |
| `LOG_FORMAT` | `text` | Log format: `text` or `json`. |
| `PG_HOST` | `localhost` | PostgreSQL host. |
| `PG_PORT` | `5432` | PostgreSQL port. |
| `PG_USER` | `postgres` | PostgreSQL user. |
| `PG_PASSWORD` | empty | PostgreSQL password. |
| `PG_DATABASE` | `postgres` | PostgreSQL database. |
| `PG_SSL` | `false` | Enable PostgreSQL TLS. |
| `PG_SSL_REJECT_UNAUTHORIZED` | `true` | Verify the PostgreSQL TLS certificate and hostname. |
| `PG_SSL_ROOT_CERT` | empty | Path to a CA certificate (PEM) used to verify the server. |
| `PG_CONNECT_TIMEOUT_MS` | `10000` | PostgreSQL connect timeout in milliseconds. |
| `PG_QUERY_TIMEOUT_MS` | `30000` | Timeout for a single database query. `0` disables it. |
| `PG_MAX_CONNECTIONS` | `10` | PostgreSQL max open connections. |
| `PUBSUB_API_ENDPOINT` | empty | Optional Pub/Sub API endpoint override. |
| `AWS_REGION` | empty | AWS region for SQS and STS. |
| `AWS_ROLE_ARN` | empty | Optional AWS role to assume before publishing to SQS. |
| `AWS_ROLE_SESSION_NAME` | `outboxer` | AWS assume-role session name. |
| `AWS_ROLE_DURATION_SECONDS` | `3600` | AWS assumed-role duration. |
| `AWS_CREDENTIAL_REFRESH_WINDOW_MS` | `300000` | Refresh assumed credentials before expiry. |
| `AWS_WEB_IDENTITY_PROVIDER` | empty | Set to `google` to assume the AWS role with a Google OIDC token (GCP to AWS). |
| `AWS_WEB_IDENTITY_AUDIENCE` | empty | Audience for the web identity token, matching the AWS IAM OIDC provider. |

## Authentication

Google Pub/Sub uses Application Default Credentials. AWS SQS uses the AWS SDK
default credential chain. If `AWS_ROLE_ARN` is set, Outboxer assumes that role
before publishing to SQS.

This covers the native cases (run on GCP and publish to Pub/Sub; run on AWS and
publish to SQS) and local development (`gcloud auth application-default login`
and/or `aws sso login`). Cross-cloud setups — publishing to SQS from GCP, or to
Pub/Sub from AWS — use workload identity federation. See
[`docs/auth.md`](docs/auth.md) for the full breakdown and required IAM.

## PostgreSQL TLS

TLS is off by default. Set `PG_SSL=true` to connect over TLS; the server must
have SSL enabled. When TLS is on, Outboxer verifies the server certificate and
hostname by default (`PG_SSL_REJECT_UNAUTHORIZED=true`):

- If the server certificate is signed by a private or self-signed CA, point
  `PG_SSL_ROOT_CERT` at the CA certificate (PEM). Otherwise the system trust
  store is used.
- The certificate must be valid for `PG_HOST`.
- To skip verification (not recommended), set `PG_SSL_REJECT_UNAUTHORIZED=false`.

## Logging

Outboxer logs to stdout using Go's `log/slog`. The level is set with
`LOG_LEVEL` (`debug`, `info`, `warn`, `error`; default `info`) and the format
with `LOG_FORMAT` (`text`, the default human-readable output, or `json` for log
aggregators). Per-event publish logs are emitted at debug level, so the default
`info` level stays quiet under load.

## Health Endpoint

The HTTP server starts only when `HEALTH_PORT`, `PORT`, or
`--health-port` is set to a positive port. It returns `200 all good` for any
request. Successful health checks are logged at debug level.

## Shutdown

Outboxer shuts down gracefully on `SIGINT` or `SIGTERM`: it stops the processing
loop, closes the database and queue clients, and exits with status `0`. A batch
that is mid-flight when shutdown begins may be interrupted; because delivery is
at-least-once, any events that were published but not yet deleted are
re-published on the next run.

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
