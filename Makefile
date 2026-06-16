.DEFAULT_GOAL := test

INTEGRATION_PG_DSN ?= postgres://outboxer:outboxer@localhost:54329/outboxer?sslmode=disable

test:
	go test ./...

test-integration:
	docker compose up -d --wait postgres
	OUTBOXER_INTEGRATION_PG_DSN='$(INTEGRATION_PG_DSN)' go test ./... -count=1

test-e2e-local:
	docker compose up -d postgres pubsub sqs
	go test -tags=e2e ./test/e2e -count=1 -v

test-integration-clean:
	docker compose down -v
