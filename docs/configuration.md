# Configuration

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

## Event table

See [Events](events.md) for the table schema, routing, and backend-specific
options.

| CLI flag | Env var | Default | Database requirement | Description |
| --- | --- | --- | --- | --- |
| `--event-table` | `EVENT_TABLE` | `events` | Table must exist. | Outbox table name. |
| `--event-id` | `EVENT_ID` | `id` | Required. | Event id column. Determines ordering and idempotency. |
| `--event-timestamp` | `EVENT_TIMESTAMP` | `timestamp` | Optional. | Event timestamp column, used for latency logs and `MAX_EVENT_AGE_MS`. |
| `--event-payload` | `EVENT_PAYLOAD` | `payload` | Required. | Event payload column. |
| `--event-target` | `EVENT_TARGET` | `target` | Required when both backends are enabled. | Backend selector column. Values `pubsub` or `sqs`. |
| `--event-destination` | `EVENT_DESTINATION` | `destination` | Required when an enabled backend has no default destination. | Pub/Sub topic name or SQS queue URL column. |
| `--event-options` | `EVENT_OPTIONS` | `options` | Optional. | Backend-specific JSON options column. Empty disables options. |

## Batch processing

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--collect-batch-target` | `COLLECT_BATCH_TARGET` | `5000` | Approximate target rows selected per batch, spread across eligible routes. Must be positive. |
| `--sqs-send-concurrency` | `SQS_SEND_CONCURRENCY` | `8` | Maximum concurrent SQS send requests. |
| `--dlq-table` | `DLQ_TABLE` | empty | Dead letter table for poison events. Empty disables the DLQ. See [Dead Letter Queue](dlq.md). |
| `--max-event-age-ms` | `MAX_EVENT_AGE_MS` | `0` | Maximum selected event age in milliseconds. `0` disables age-based poison. Requires `EVENT_TIMESTAMP`. |
| `--error-cooldown-ms` | `ERROR_COOLDOWN_MS` | `5000` | Sleep after batch or database errors in milliseconds. |
| `--poll-interval-ms` | `POLL_INTERVAL_MS` | `0` | Idle wait after an empty batch in milliseconds. The default keeps polling immediately. When `> 0`, the wait is interrupted by a `LISTEN`/`NOTIFY` wake-up if the optional trigger is installed. See [Notifications](notifications.md). |
| `--watchdog-interval-ms` | `WATCHDOG_INTERVAL_MS` | `600000` | Watchdog interval in milliseconds. Must be at least 10x `POLL_INTERVAL_MS` when polling is enabled. |
| `--publish-timeout-ms` | `PUBLISH_TIMEOUT_MS` | `30000` | Timeout for a single publish call in milliseconds. Must be positive. |
| `--publish-result-grace-ms` | `PUBLISH_RESULT_GRACE_MS` | `5000` | Extra wait after provider publish timeout for async Pub/Sub publish results. |
| `--stats-interval-ms` | `STATS_INTERVAL_MS` | `10000` | Periodic statistics logging interval in milliseconds. `0` disables statistics. See [Statistics](#statistics). |
| `--notify-channel` | `NOTIFY_CHANNEL` | `outboxer_events` | PostgreSQL `LISTEN` channel for the optional new-event notification trigger. Only used when `POLL_INTERVAL_MS > 0`. See [Notifications](notifications.md). |

## HTTP / health

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--health-port` | `HEALTH_PORT`, `PORT` | `PORT` or `0` | HTTP health server port. `0` disables the server. |

The HTTP server starts only when `HEALTH_PORT`, `PORT`, or `--health-port` is set
to a positive port. It returns `200 all good` for any request. Successful health
checks are logged at debug level.

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
| `--pg-connect-timeout-ms` | `PG_CONNECT_TIMEOUT_MS` | `10000` | PostgreSQL connect timeout in milliseconds. |
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
| `--aws-role-duration-seconds` | `AWS_ROLE_DURATION_SECONDS` | `3600` | AWS assumed-role duration in seconds. |
| `--aws-credential-refresh-window-ms` | `AWS_CREDENTIAL_REFRESH_WINDOW_MS` | `300000` | Refresh assumed credentials before expiry in milliseconds. |
| `--aws-web-identity-provider` | `AWS_WEB_IDENTITY_PROVIDER` | empty | Set to `google` to assume the AWS role with a Google OIDC token (GCP to AWS). |
| `--aws-web-identity-audience` | `AWS_WEB_IDENTITY_AUDIENCE` | empty | Audience for the web identity token, matching the AWS IAM OIDC provider. |

Authentication and cross-cloud (workload identity federation) setups are covered
in [Authentication](auth.md).
