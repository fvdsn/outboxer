set dotenv-load

integration_pg_dsn := env_var_or_default("INTEGRATION_PG_DSN", "postgres://outboxer:outboxer@localhost:54329/outboxer?sslmode=disable")
image := env_var_or_default("IMAGE", "outboxer:local")

# List available commands.
default:
    @just --list

# Run the application locally.
run:
    go run ./cmd/outboxer

# Build the app without leaving a local artifact.
build:
    go build -o /tmp/outboxer-go-check ./cmd/outboxer

# Build a local binary.
binary:
    go build -o outboxer ./cmd/outboxer

# Build the Docker image.
docker-build:
    docker build -t {{image}} .

# Run unit tests.
test:
    go test ./...

# Run unit tests with verbose output.
test-verbose:
    go test ./... -v

# Start the integration test database.
db-up:
    docker compose up -d --wait postgres

# Stop and remove the integration test database.
db-down:
    docker compose down -v

# Show integration database logs.
db-logs:
    docker compose logs -f postgres

# Run integration tests against the Docker Compose database.
integration: db-up
    OUTBOXER_INTEGRATION_PG_DSN='{{integration_pg_dsn}}' go test ./... -count=1 -v

# Start the full local end-to-end stack.
e2e-local-up:
    docker compose up -d postgres pubsub sqs

# Run process-level E2E tests against local Postgres, Pub/Sub emulator, and ElasticMQ SQS.
e2e-local: e2e-local-up
    go test -tags=e2e ./test/e2e -count=1 -v

# Alias for integration tests.
e2e: integration

# Run integration tests and clean up the database afterwards.
integration-clean:
    just integration
    just db-down

# Alias for clean e2e tests.
e2e-clean: integration-clean

# Run local emulator E2E tests and clean up afterwards.
e2e-local-clean:
    just e2e-local
    just db-down

# Format Go code.
fmt:
    gofmt -w ./cmd/outboxer/*.go ./internal/outboxer/*.go ./test/e2e/*.go

# Tidy Go modules.
tidy:
    go mod tidy

# Run go vet.
vet:
    go vet ./...

# Run golangci-lint (pinned to the version used in CI).
lint:
    go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run ./...

# Scan dependencies for known vulnerabilities.
vuln:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Format, tidy, and run every static check plus unit tests and build.
check: fmt tidy vet lint vuln test build

# Run the full CI suite locally, including the integration tests.
ci: fmt tidy vet lint vuln integration build
