package outboxer

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const insertDLQSQL = `INSERT INTO "dead_letters" ("event") VALUES ($1::jsonb)`

type dlqPayloadMatcher struct {
	t     *testing.T
	check func(map[string]any) bool
}

func (m dlqPayloadMatcher) Match(value driver.Value) bool {
	m.t.Helper()

	var raw string
	switch typed := value.(type) {
	case string:
		raw = typed
	case []byte:
		raw = string(typed)
	default:
		m.t.Errorf("DLQ payload has type %T, want string or []byte", value)
		return false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		m.t.Errorf("DLQ payload is not JSON: %v\n%s", err, raw)
		return false
	}
	return m.check(payload)
}

func TestCheckDLQWorksValidatesConfiguredTable(t *testing.T) {
	cfg := testConfig()
	cfg.DLQTable = "dead_letters"
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()

	mock.ExpectQuery(`SELECT "id", "event"::jsonb FROM "dead_letters" LIMIT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event"}))

	if err := a.checkDLQWorks(context.Background()); err != nil {
		t.Fatalf("checkDLQWorks returned error: %v", err)
	}
}

func TestDeadLetterPayloadIncludesResolvedRouteDefaults(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = true
	cfg.SQSEnabled = false
	cfg.EventTarget = ""
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = "projects/outboxer-test/topics/default-topic"
	a := &app{cfg: cfg}

	payload := a.deadLetterPayload(poisonEvent{
		evt:   event{columns: map[string]any{"id": "event-1", "payload": "hello"}},
		error: "Pub/Sub message is invalid",
	})

	if payload["source_table"] != "events" {
		t.Fatalf("unexpected source_table: %#v", payload["source_table"])
	}
	if _, err := time.Parse(time.RFC3339Nano, payload["dead_lettered_at"].(string)); err != nil {
		t.Fatalf("dead_lettered_at is not RFC3339Nano: %v", err)
	}
	if payload["target"] != "pubsub" {
		t.Fatalf("expected resolved target pubsub, got %#v", payload["target"])
	}
	if payload["destination"] != "projects/outboxer-test/topics/default-topic" {
		t.Fatalf("expected resolved default destination, got %#v", payload["destination"])
	}
	if payload["error"] != "Pub/Sub message is invalid" {
		t.Fatalf("unexpected error field: %#v", payload["error"])
	}
	if _, ok := payload["reason"]; ok {
		t.Fatal("DLQ payload must not expose a stable machine-readable reason field")
	}

	original, ok := payload["original_event"].(map[string]any)
	if !ok {
		t.Fatalf("original_event has type %T", payload["original_event"])
	}
	if original["id"] != "event-1" || original["payload"] != "hello" {
		t.Fatalf("unexpected original_event: %#v", original)
	}
}

func TestProcessOneBatchDeadLettersContentPoisonAndDeletesConfirmedSendTogether(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.DLQTable = "dead_letters"
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("poison", "sqs", "queue-a", "", nil, nil).
		AddRow("confirmed", "sqs", "queue-a", "payload", nil, nil)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(insertDLQSQL).WithArgs(dlqPayloadMatcher{t: t, check: func(payload map[string]any) bool {
		if payload["target"] != "sqs" || payload["destination"] != "queue-a" {
			t.Errorf("unexpected resolved route in DLQ payload: %#v", payload)
			return false
		}
		errorText, _ := payload["error"].(string)
		if !strings.Contains(errorText, "invalid for SQS") {
			t.Errorf("unexpected DLQ error: %#v", payload["error"])
			return false
		}
		if _, ok := payload["reason"]; ok {
			t.Errorf("DLQ payload must not include reason: %#v", payload)
			return false
		}
		original, ok := payload["original_event"].(map[string]any)
		if !ok || original["id"] != "poison" || original["target"] != "sqs" || original["destination"] != "queue-a" {
			t.Errorf("unexpected original event: %#v", payload["original_event"])
			return false
		}
		return true
	}}).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(deleteTwoSQL).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
	if len(sqs.requests) != 1 || len(sqs.requests[0].entries) != 1 || sqs.requests[0].entries[0].ID != "confirmed" {
		t.Fatalf("expected only confirmed event to be sent, got %#v", sqs.requests)
	}
}

func TestProcessOneBatchDeadLettersPubSubLocalPoison(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.DLQTable = "dead_letters"
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub

	rows := mockEventRows().AddRow("poison", "pubsub", "bad/topic", "payload", nil, nil)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(insertDLQSQL).WithArgs(dlqPayloadMatcher{t: t, check: func(payload map[string]any) bool {
		if payload["target"] != "pubsub" || payload["destination"] != "bad/topic" {
			t.Errorf("unexpected resolved route in DLQ payload: %#v", payload)
			return false
		}
		errorText, _ := payload["error"].(string)
		if !strings.Contains(errorText, "topic name is syntactically invalid") {
			t.Errorf("unexpected DLQ error: %#v", payload["error"])
			return false
		}
		return true
	}}).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(deleteOneSQL).WithArgs("poison").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected Pub/Sub local poison not to publish, got %#v", pubsub.messages)
	}
}

func TestProcessOneBatchRollsBackWhenDeadLetterInsertFails(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.DLQTable = "dead_letters"
	expectedErr := errors.New("dlq insert failed")
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().AddRow("poison", "sqs", "queue-a", "", nil, nil)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(insertDLQSQL).WithArgs(sqlmock.AnyArg()).WillReturnError(expectedErr)
	mock.ExpectRollback()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected DLQ database error, got %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected poison event not to be sent to SQS, got %#v", sqs.requests)
	}
}
