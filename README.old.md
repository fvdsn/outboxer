for the transactional outbox pattern. It reads events
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
    options jsonb
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
| `options` | never | Backend-specific JSON options such as ordering keys and attributes. |

A minimal single-backend table can therefore be just `id` and `payload`, with a
default destination configured.

### Pub/Sub Example

```sql
INSERT INTO events (id, timestamp, payload, target, destination, options)
VALUES (
    'event-1',
    now(),
    '{"type":"user.created","id":"123"}',
    'pubsub',
    'user-events',
    '{"pubsub":{"orderingKey":"user-123","attributes":{"source":"users"}}}'
);
```

### SQS Example

```sql
INSERT INTO events (id, timestamp, payload, target, destination, options)
VALUES (
    'event-2',
    now(),
    '{"type":"invoice.created","id":"456"}',
    'sqs',
    'https://sqs.eu-west-1.amazonaws.com/123456789012/invoices',
    '{"sqs":{"attributes":{"source":{"DataType":"String","StringValue":"billing"}}}}'
);
```

Pub/Sub attributes under `options.pubsub.attributes` must be strings; non-string
values are dropped and logged. SQS attributes under `options.sqs.attributes` use
AWS's native `MessageAttributeValue` JSON shape.

### SQS FIFO Queues

Outboxer detects FIFO queues from the `.fifo` suffix on the queue URL, so
standard and FIFO queues are handled correctly without extra configuration:

- **FIFO queues** receive a `MessageGroupId` from
  `options.sqs.messageGroupId`, or a stable synthetic group from the event `id`
  when it has none. `MessageDeduplicationId` is also derived from the event `id`.
  Using the event id for deduplication means re-sends after a crash are
  deduplicated by SQS, so content-based deduplication is not required on the
  queue.
- **Standard queues** never receive a `MessageGroupId` or
  `MessageDeduplicationId`.

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

### Event table

| CLI flag | Env var | Default | Database requirement | Description |
| --- | --- | --- | --- | --- |
| `--event-table` | `EVENT_TABLE` | `events` | Table must exist. | Outbox table name. |
| `--event-id` | `EVENT_ID` | `id` | Required. | Event id column. |
| `--event-timestamp` | `EVENT_TIMESTAMP` | `timestamp` | Optional. | Event timestamp column, used for latency logs. |
| `--event-payload` | `EVENT_PAYLOAD` | `payload` | Required. | Event payload column. |
| `--event-target` | `EVENT_TARGET` | `target` | Required when both backends are enabled. | Backend selector column. Values `pubsub` or `sqs`. |
| `--event-destination` | `EVENT_DESTINATION` | `destination` | Required when an enabled backend has no default destination. | Pub/Sub topic name or SQS queue URL column. |
| `--event-options` | `EVENT_OPTIONS` | `options` | Optional. | Backend-specific JSON options column. Empty disables options. |

### Batch processing

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--collect-batch-target` | `COLLECT_BATCH_TARGET` | `5000` | Approximate target rows selected per batch, spread across eligible routes. |
| `--sqs-send-concurrency` | `SQS_SEND_CONCURRENCY` | `8` | Maximum concurrent SQS send requests. |
| `--error-cooldown-ms` | `ERROR_COOLDOWN_MS` | `5000` | Sleep after batch or database errors in milliseconds. |
| `--poll-interval-ms` | `POLL_INTERVAL_MS` | `0` | Sleep after an empty batch in milliseconds. The default keeps polling immediately. |
| `--watchdog-interval-ms` | `WATCHDOG_INTERVAL_MS` | `600000` | Watchdog interval in milliseconds. Must be at least 10x `POLL_INTERVAL_MS` when polling is enabled. |
| `--publish-timeout-ms` | `PUBLISH_TIMEOUT_MS` | `30000` | Timeout for a single publish call in milliseconds. Must be positive. |
| `--publish-result-grace-ms` | `PUBLISH_RESULT_GRACE_MS` | `5000` | Extra wait after provider publish timeout for async Pub/Sub publish results. |

### HTTP / health

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--health-port` | `HEALTH_PORT`, `PORT` | `PORT` or `0` | HTTP health server port. `0` disables the server. |

### Logging

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--log-level` | `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error`. |
| `--log-format` | `LOG_FORMAT` | `text` | Log format: `text` or `json`. |

### PostgreSQL

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--pg-host` | `PG_HOST` | `localhost` | PostgreSQL host. |
| `--pg-port` | `PG_PORT` | `5432` | PostgreSQL port. |
| `--pg-user` | `PG_USER` | `postgres` | PostgreSQL user. |
| `--pg-password` | `PG_PASSWORD` | empty | PostgreSQL password. |
| `--pg-database` | `PG_DATABASE` | `postgres` | PostgreSQL database. |
| `--pg-ssl` | `PG_SSL` | `false` | Enable PostgreSQL TLS. |
| `--pg-ssl-reject-unauthorized` | `PG_SSL_REJECT_UNAUTHORIZED` | `true` | Verify PostgreSQL TLS certificate and hostname. |
| `--pg-ssl-root-cert` | `PG_SSL_ROOT_CERT` | empty | Path to a CA certificate (PEM) used to verify the server. |
| `--pg-connect-timeout-ms` | `PG_CONNECT_TIMEOUT_MS` | `10000` | PostgreSQL connect timeout in milliseconds. |
| `--pg-query-timeout-ms` | `PG_QUERY_TIMEOUT_MS` | `30000` | Timeout for a single database query in milliseconds. `0` disables it. |

### Google Pub/Sub

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--pubsub-enabled` | `PUBSUB_ENABLED` | `false` | Enable publishing to Google Pub/Sub. |
| `--default-pubsub-topic` | `DEFAULT_PUBSUB_TOPIC` | `default` | Pub/Sub topic used when an event has no destination. |
| `--pubsub-project-id` | `PUBSUB_PROJECT_ID` | empty | Google Cloud project for Pub/Sub. Detected from ADC when empty. |
| `--pubsub-api-endpoint` | `PUBSUB_API_ENDPOINT` | empty | Optional Pub/Sub API endpoint override. |

### AWS SQS

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--sqs-enabled` | `SQS_ENABLED` | `false` | Enable publishing to AWS SQS. |
| `--default-sqs-queue-url` | `DEFAULT_SQS_QUEUE_URL` | empty | SQS queue URL used when an event has no destination. |
| `--sqs-api-endpoint` | `SQS_API_ENDPOINT` | empty | Optional SQS API endpoint override, useful for local emulators such as ElasticMQ. |
| `--aws-region` | `AWS_REGION` | empty | AWS region for SQS and STS. |
| `--aws-role-arn` | `AWS_ROLE_ARN` | empty | Optional AWS role to assume before publishing to SQS. |
| `--aws-role-session-name` | `AWS_ROLE_SESSION_NAME` | `outboxer` | AWS assume-role session name. |
| `--aws-role-duration-seconds` | `AWS_ROLE_DURATION_SECONDS` | `3600` | AWS assumed-role duration in seconds. |
| `--aws-credential-refresh-window-ms` | `AWS_CREDENTIAL_REFRESH_WINDOW_MS` | `300000` | Refresh assumed credentials before expiry in milliseconds. |
| `--aws-web-identity-provider` | `AWS_WEB_IDENTITY_PROVIDER` | empty | Set to `google` to assume the AWS role with a Google OIDC token (GCP to AWS). |
| `--aws-web-identity-audience` | `AWS_WEB_IDENTITY_AUDIENCE` | empty | Audience for the web identity token, matching the AWS IAM OIDC provider. |

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

### Run under a supervisor

Outboxer must run under a process supervisor that restarts it on exit
(Kubernetes, ECS, systemd, etc.). It deliberately exits the process — rather than
trying to recover in place — on unrecoverable conditions: a detected watchdog
deadlock, an unrecoverable queue-client state, or an unknown in-flight ordered
publish. Exiting and being restarted with fresh clients is the safe recovery
path; without a supervisor the process would simply stay down.

## Layout

```text
cmd/outboxer/         executable entrypoint
docs/                 user-facing guides
examples/kubernetes/  sample Kubernetes manifests
examples/terraform/   sample cloud deployment examples
internal/outboxer/    service implementation
specs/                implementation requirements and use cases
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

## Container Image

Release images are published to GitHub Container Registry:

```text
ghcr.io/fvdsn/outboxer:v0.1.0
ghcr.io/fvdsn/outboxer:0.1.0
ghcr.io/fvdsn/outboxer:latest
```

Images are built for `linux/amd64` and `linux/arm64`. The runtime image uses a
small Alpine Linux base, runs as a non-root user, and includes CA certificates
for TLS connections to PostgreSQL, Pub/Sub, SQS, and metadata services.

To build locally:

```sh
docker build -t outboxer:local .
```

## Deployment

Deployment guidance and sample deployment examples are available in
[`docs/deployment.md`](docs/deployment.md). The examples cover GCP Cloud Run,
GKE, AWS ECS Fargate, and EKS.

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

## Local E2E Test

The local E2E suite runs the real Outboxer binary against real PostgreSQL plus
local queue emulators:

- Google Pub/Sub emulator on `localhost:8085`
- ElasticMQ SQS on `localhost:9324`

It creates Pub/Sub topics/subscriptions and SQS standard/FIFO queues, inserts
events into PostgreSQL, starts Outboxer, and verifies the messages received from
the queues.

```sh
just e2e-local
```

This runs:

```sh
go test -tags=e2e ./test/e2e -count=1 -v
```

To stop and remove the local services:

```sh
just db-down
```

Useful commands are listed with:

```sh
just
```
