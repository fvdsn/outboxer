set dotenv-load

integration_pg_dsn := env_var_or_default("INTEGRATION_PG_DSN", "postgres://outboxer:outboxer@localhost:54329/outboxer?sslmode=disable")
image := env_var_or_default("IMAGE", "outboxer-go:local")

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
    go build -o outboxer-go ./cmd/outboxer

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

# Alias for integration tests.
e2e: integration

# Run integration tests and clean up the database afterwards.
integration-clean:
    just integration
    just db-down

# Alias for clean e2e tests.
e2e-clean: integration-clean

# Format Go code.
fmt:
    gofmt -w ./cmd/outboxer/*.go ./internal/outboxer/*.go

# Tidy Go modules.
tidy:
    go mod tidy

# Format, tidy, test, and build.
check: fmt tidy test build
