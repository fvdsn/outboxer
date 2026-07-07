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

# Format Go code (the same directories CI's gofmt check verifies).
fmt:
    gofmt -w ./cmd ./internal ./test

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

# --- Cloud integration tests (ephemeral, real infrastructure) ---------------

gcp_project := env_var_or_default("OUTBOXER_GCP_PROJECT", "")
gcp_region := env_var_or_default("OUTBOXER_GCP_REGION", "europe-west1")
cloud_image_tag := `git rev-parse --short HEAD 2>/dev/null || echo dev`

# Deploy the ephemeral GCP Cloud Run stack (Cloud SQL creation takes ~10 min).
cloud-gcp-cloudrun-up:
    @test -n "{{gcp_project}}" || (echo "set OUTBOXER_GCP_PROJECT in .env or the environment" && exit 1)
    cd deploy/gcp-cloudrun && terraform init -input=false
    cd deploy/gcp-cloudrun && terraform apply -input=false -auto-approve \
        -var project_id={{gcp_project}} -var region={{gcp_region}} \
        -target=google_project_service.apis -target=google_artifact_registry_repository.outboxer
    gcloud auth configure-docker {{gcp_region}}-docker.pkg.dev --quiet
    docker build --platform linux/amd64 -t {{gcp_region}}-docker.pkg.dev/{{gcp_project}}/outboxer/outboxer:{{cloud_image_tag}} .
    docker push {{gcp_region}}-docker.pkg.dev/{{gcp_project}}/outboxer/outboxer:{{cloud_image_tag}}
    cd deploy/gcp-cloudrun && terraform apply -input=false -auto-approve \
        -var project_id={{gcp_project}} -var region={{gcp_region}} \
        -var image={{gcp_region}}-docker.pkg.dev/{{gcp_project}}/outboxer/outboxer:{{cloud_image_tag}}
    gcloud run jobs execute outboxer-init --project {{gcp_project}} --region {{gcp_region}} --wait
    cd deploy/gcp-cloudrun && terraform output -json > ../../test/cloud/gcpcloudrun/tfoutputs.json

# Run the functional cloud scenarios against the deployed stack.
cloud-gcp-cloudrun-test:
    go test -tags=cloud ./test/cloud/gcpcloudrun -run TestGCPCloudRunSmoke -count=1 -timeout 15m -v

# Run the performance scenario (OUTBOXER_CLOUD_PERF_EVENTS overrides the volume).
cloud-gcp-cloudrun-perf:
    go test -tags=cloud ./test/cloud/gcpcloudrun -run TestGCPCloudRunPerf -count=1 -timeout 60m -v

# Destroy the GCP Cloud Run stack.
cloud-gcp-cloudrun-down:
    cd deploy/gcp-cloudrun && terraform destroy -input=false -auto-approve \
        -var project_id={{gcp_project}} -var region={{gcp_region}}
    rm -f test/cloud/gcpcloudrun/tfoutputs.json

# Emergency sweep: list every resource labeled outboxer-test in the project,
# for manual cleanup if the local Terraform state is ever lost.
cloud-gcp-orphans:
    gcloud asset search-all-resources --scope=projects/{{gcp_project}} \
        --query="labels.outboxer-test=true" \
        --format="table(assetType, displayName, location)"
