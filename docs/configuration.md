# Configuration

Outboxer reads environment variables and also loads a local `.env` file when
present. Every configuration value can also be set with a CLI flag.

Configuration precedence is:

1. CLI flags
2. environment variables or `.env`
3. defaults

An environment variable is parsed exactly like the equivalent CLI flag, so
`FOO=bar` behaves identically to `--foo=bar` and an invalid value (a bad integer,
an unparseable boolean, an out-of-range port) is rejected at startup rather than
silently ignored. An **empty** value (`FOO=` or `--foo=`) is always an error;
optional columns and tables are omitted with the explicit `disabled` value
instead (for example `EVENT_OPTIONS=disabled`).

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

## Event table

See [Events](events.md) for the table schema, routing, and backend-specific
options.

| CLI flag | Env var | Default | Database requirement | Description |
| --- | --- | --- | --- | --- |
| `--event-table` | `EVENT_TABLE` | `events` | Table must exist. | Outbox table name. |
| `--event-id` | `EVENT_ID` | `id` | Required. | Event id column. Determines ordering and idempotency. |
| `--event-timestamp` | `EVENT_TIMESTAMP` | `timestamp` | Optional. | Event timestamp column, used for latency logs and `MAX_EVENT_AGE_MS`. Set to `disabled` to omit it. |
| `--event-payload` | `EVENT_PAYLOAD` | `payload` | Required. | Event payload column. |
| `--event-target` | `EVENT_TARGET` | `target` | Required when both backends are enabled. | Backend selector column. Values `pubsub` or `sqs`. Set to `disabled` to omit it. |
| `--event-destination` | `EVENT_DESTINATION` | `destination` | Required when an enabled backend has no default destination. | Pub/Sub topic name or SQS queue URL column. Set to `disabled` to omit it. |
| `--event-options` | `EVENT_OPTIONS` | `options` | Optional. | Backend-specific JSON options column. Set to `disabled` to omit it. |

## Batch processing

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--collect-batch-target` | `COLLECT_BATCH_TARGET` | `10000` | Approximate target rows selected per batch. Must be positive. Every eligible route (distinct target and destination pair with pending events) gets an even share of the target, at least one row; a busy route does not borrow an idle route's share within a batch. Throughput rises with batch size, but peak in-flight memory is roughly batch × payload size and a failed batch is redelivered whole — size it to your events (the default holds ~100 MB at 10 KB payloads). |
| `--dlq-table` | `DLQ_TABLE` | `disabled` | Dead letter table for poison events. Defaults to `disabled`; set a table name to enable. See [Dead Letter Queue](dlq.md). |
| `--max-event-age-ms` | `MAX_EVENT_AGE_MS` | `0` | Maximum selected event age in milliseconds. `0` disables age-based poison. Requires `EVENT_TIMESTAMP`. |
| `--poll-interval-ms` | `POLL_INTERVAL_MS` | `1000` | Idle wait after an empty batch in milliseconds, cut short by a `LISTEN`/`NOTIFY` wake-up via the notification trigger that `init` provisions. Set to `0` to poll continuously with no sleep. See [Notifications](notifications.md). |
| `--publish-timeout-ms` | `PUBLISH_TIMEOUT_MS` | `30000` | Timeout for a single publish call in milliseconds. Must be positive. |

## HTTP / health

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--health-port` | `HEALTH_PORT`, `PORT` | `PORT` or `0` | HTTP health server port. `0` disables the server. |

The HTTP server starts only when `HEALTH_PORT`, `PORT`, or `--health-port` is set
to a positive port. It serves `/healthz` (batch-staleness health; `/health` is an alias for platforms whose edge intercepts `/healthz`, such as Cloud Run), `/metrics`
(Prometheus), and `200 all good` for any other path as a pure liveness signal.
See [Observability](observability.md). Successful health checks are logged at
debug level.

## Logging

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--log-level` | `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error`. |
| `--log-format` | `LOG_FORMAT` | `text` | Log format: `text` or `json`. |

Outboxer logs to stdout using Go's `log/slog`. `text` is human-readable; `json`
suits log aggregators. Per-event publish logs are emitted at debug level, so the
default `info` level stays quiet under load. The full log catalog and the
periodic `Statistics` fields are documented in [Logging](logs.md).

## PostgreSQL

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--pg-host` | `PG_HOST` | `localhost` | PostgreSQL host. |
| `--pg-port` | `PG_PORT` | `5432` | PostgreSQL port. |
| `--pg-user` | `PG_USER` | `postgres` | PostgreSQL user. |
| `--pg-password` | `PG_PASSWORD` | empty | PostgreSQL password. |
| `--pg-database` | `PG_DATABASE` | `postgres` | PostgreSQL database. |
| `--pg-schema` | `PG_SCHEMA` | `public` | PostgreSQL schema containing the outbox table, optional DLQ table, and notification function. |
| `--pg-ssl` | `PG_SSL` | `false` | Enable PostgreSQL TLS. |
| `--pg-ssl-reject-unauthorized` | `PG_SSL_REJECT_UNAUTHORIZED` | `true` | Verify PostgreSQL TLS certificate and hostname. |
| `--pg-ssl-root-cert` | `PG_SSL_ROOT_CERT` | empty | Path to a CA certificate (PEM) used to verify the server. |
| `--pg-query-timeout-ms` | `PG_QUERY_TIMEOUT_MS` | `30000` | Timeout for a single database query in milliseconds. `0` disables it. |

### TLS

TLS is off by default. Set `PG_SSL=true` to connect over TLS; the server must
have SSL enabled. When TLS is on, Outboxer verifies the server certificate and
hostname by default (`PG_SSL_REJECT_UNAUTHORIZED=true`):

- If the server certificate is signed by a private or self-signed CA, point
  `PG_SSL_ROOT_CERT` at the CA certificate (PEM). Otherwise the system trust
  store is used.
- The certificate must be valid for `PG_HOST`.
- To skip verification (not recommended), set `PG_SSL_REJECT_UNAUTHORIZED=false`.

### Provisioning (init command)

These settings are used only by `outboxer init` (see
[Provisioning](provisioning.md)). They are ignored by the relay.

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--apply` | — | off | Execute the generated SQL against the database instead of printing it to stdout. |
| `--pg-init-user` | `PG_INIT_USER` | empty | Provisioning role to connect as for `--apply`. When set, `init` also creates and grants to the run role; when empty, `init` connects as `PG_USER` and only creates schema objects. |
| `--pg-init-password` | `PG_INIT_PASSWORD` | empty | Password for the provisioning role. |
| `--pg-producer-roles` | `PG_PRODUCER_ROLES` | empty | Comma-separated existing roles granted `SELECT, INSERT` on the event table. Grant-only; `init` never creates them. |

## Google Pub/Sub

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--pubsub-enabled` | `PUBSUB_ENABLED` | `false` | Enable publishing to Google Pub/Sub. |
| `--default-pubsub-topic` | `DEFAULT_PUBSUB_TOPIC` | `default` | Pub/Sub topic used when an event has no destination. |
| `--pubsub-destinations` | `PUBSUB_DESTINATIONS` | empty | Comma-separated Pub/Sub destinations this process owns. Empty means all Pub/Sub destinations. |
| `--pubsub-project-id` | `PUBSUB_PROJECT_ID` | empty | Google Cloud project for Pub/Sub. Detected from ADC when empty. |
| `--pubsub-api-endpoint` | `PUBSUB_API_ENDPOINT` | empty | Optional Pub/Sub API endpoint override. |

## AWS SQS

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--sqs-enabled` | `SQS_ENABLED` | `false` | Enable publishing to AWS SQS. |
| `--default-sqs-queue-url` | `DEFAULT_SQS_QUEUE_URL` | empty | SQS queue URL used when an event has no destination. |
| `--sqs-destinations` | `SQS_DESTINATIONS` | empty | Comma-separated SQS destinations this process owns. Empty means all SQS destinations. |
| `--sqs-api-endpoint` | `SQS_API_ENDPOINT` | empty | Optional SQS API endpoint override, useful for local emulators such as ElasticMQ. |
| `--aws-region` | `AWS_REGION` | empty | AWS region for SQS and STS. |
| `--aws-role-arn` | `AWS_ROLE_ARN` | empty | Optional AWS role to assume before publishing to SQS. |
| `--aws-role-session-name` | `AWS_ROLE_SESSION_NAME` | `outboxer` | AWS assume-role session name. |
| `--aws-web-identity-provider` | `AWS_WEB_IDENTITY_PROVIDER` | empty | Set to `google` to assume the AWS role with a Google OIDC token (GCP to AWS). |
| `--aws-web-identity-audience` | `AWS_WEB_IDENTITY_AUDIENCE` | empty | Audience for the web identity token, matching the AWS IAM OIDC provider. |

Authentication and cross-cloud (workload identity federation) setups are covered
in [Authentication](auth.md).

## Fixed settings

Values that have one good answer are constants, not configuration — a smaller
surface beats a tunable one. These were knobs in earlier releases; each was
decided by measurement or design and removed. Setting one of the retired
environment variables is a startup error, so a stale deployment manifest fails
loudly instead of being silently ignored.

| Was | Now |
| --- | --- |
| `SQS_SEND_CONCURRENCY` | 128 concurrent sends, the fastest measured setting; the HTTP pool is sized to match. |
| `BACKLOG_COUNT_LIMIT` | The backlog probe scans at most 100,000 rows. |
| `ERROR_COOLDOWN_MS` | 5 s sleep after a failed batch. |
| `PUBLISH_RESULT_GRACE_MS` | 5 s extra wait for async publish results. |
| `STATS_INTERVAL_MS` | Statistics log every 10 s. |
| `WATCHDOG_INTERVAL_MS` | 10 min, or 10× `POLL_INTERVAL_MS` when larger. |
| `HEALTH_STALE_AFTER_MS` | `/healthz` turns unhealthy after 5 min without a committed batch, or 10× `POLL_INTERVAL_MS` when larger. |
| `PG_CONNECT_TIMEOUT_MS` | 10 s. |
| `AWS_ROLE_DURATION_SECONDS` | 1 h assumed-role sessions. |
| `AWS_CREDENTIAL_REFRESH_WINDOW_MS` | Credentials refresh 5 min before expiry. |
| `NOTIFY_CHANNEL` | Derived from the event table as `outboxer_<table>`, so multiple tables in one database need no coordination. Re-run `init` after upgrading if your table is not named `events`. |
