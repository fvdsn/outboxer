# outboxer-go

Go port of the `outboxer-js` service.

It reads events from a PostgreSQL outbox table, publishes them to Google Pub/Sub
or AWS SQS, and deletes successfully published rows. The runtime configuration
uses the same environment variable names and defaults as the JavaScript service.

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
