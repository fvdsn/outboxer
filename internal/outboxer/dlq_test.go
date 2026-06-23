package outboxer

import (
	"context"
	"database/sql"
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

type dlqMetadataRow struct {
	name            string
	typeName        string
	notNull         bool
	defaultExpr     string
	identity        string
	generated       string
	canInsertColumn bool
}

func (r dlqMetadataRow) toMetadata() dlqColumnMetadata {
	return dlqColumnMetadata{
		name:            r.name,
		typeName:        r.typeName,
		notNull:         r.notNull,
		defaultExpr:     sqlNullString(r.defaultExpr),
		identity:        r.identity,
		generated:       r.generated,
		canInsertColumn: r.canInsertColumn,
	}
}

func dlqMetadataRows(rows ...dlqMetadataRow) *sqlmock.Rows {
	sqlRows := sqlmock.NewRows([]string{
		"relkind",
		"can_insert_table",
		"attname",
		"typname",
		"attnotnull",
		"default_expr",
		"attidentity",
		"attgenerated",
		"can_insert_column",
	})
	for _, row := range rows {
		sqlRows.AddRow("r", true, row.name, row.typeName, row.notNull, nullableStringValue(row.defaultExpr), row.identity, row.generated, row.canInsertColumn)
	}
	return sqlRows
}

func sqlNullString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func nullableStringValue(value string) any {
	if value == "" {
		return nil
	}
	return value
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

	mock.ExpectQuery(dlqMetadataSQL).
		WithArgs(`"dead_letters"`).
		WillReturnRows(dlqMetadataRows(
			dlqMetadataRow{name: "id", typeName: "int8", notNull: true, defaultExpr: "nextval('dead_letters_id_seq'::regclass)", canInsertColumn: true},
			dlqMetadataRow{name: "event", typeName: "jsonb", notNull: true, canInsertColumn: true},
		))

	if err := a.checkDLQWorks(context.Background()); err != nil {
		t.Fatalf("checkDLQWorks returned error: %v", err)
	}
}

func TestValidateDLQTableMetadataRejectsUninsertableTableShapes(t *testing.T) {
	validID := dlqMetadataRow{name: "id", typeName: "int8", notNull: true, defaultExpr: "nextval('dead_letters_id_seq'::regclass)", canInsertColumn: true}
	validEvent := dlqMetadataRow{name: "event", typeName: "jsonb", notNull: true, canInsertColumn: true}

	tests := []struct {
		name     string
		metadata dlqTableMetadata
		want     string
	}{
		{
			name:     "missing table",
			metadata: dlqTableMetadata{},
			want:     "does not exist",
		},
		{
			name: "view",
			metadata: dlqTableMetadata{relkind: "v", canInsertTable: true, columns: []dlqColumnMetadata{
				validID.toMetadata(), validEvent.toMetadata(),
			}},
			want: "ordinary or partitioned table",
		},
		{
			name: "missing table insert privilege",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: false, columns: []dlqColumnMetadata{
				validID.toMetadata(), validEvent.toMetadata(),
			}},
			want: "missing INSERT privilege",
		},
		{
			name: "missing id",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				validEvent.toMetadata(),
			}},
			want: "missing required column: id",
		},
		{
			name: "id cannot be omitted",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				{name: "id", typeName: "int8", notNull: true, canInsertColumn: true},
				validEvent.toMetadata(),
			}},
			want: "column id must be nullable",
		},
		{
			name: "missing event",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				validID.toMetadata(),
			}},
			want: "missing required column: event",
		},
		{
			name: "event generated",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				validID.toMetadata(),
				{name: "event", typeName: "jsonb", notNull: true, generated: "s", canInsertColumn: true},
			}},
			want: "event must accept inserted values",
		},
		{
			name: "event wrong type",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				validID.toMetadata(),
				{name: "event", typeName: "text", notNull: true, canInsertColumn: true},
			}},
			want: "must be json or jsonb",
		},
		{
			name: "event missing column insert privilege",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				validID.toMetadata(),
				{name: "event", typeName: "jsonb", notNull: true, canInsertColumn: false},
			}},
			want: "column event",
		},
		{
			name: "extra required column",
			metadata: dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
				validID.toMetadata(), validEvent.toMetadata(),
				{name: "tenant", typeName: "text", notNull: true, canInsertColumn: true},
			}},
			want: "required columns without defaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDLQTableMetadata("dead_letters", tt.metadata)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestValidateDLQTableMetadataAllowsInsertableExtraColumns(t *testing.T) {
	metadata := dlqTableMetadata{relkind: "r", canInsertTable: true, columns: []dlqColumnMetadata{
		{name: "id", typeName: "int8", notNull: true, identity: "d", canInsertColumn: true},
		{name: "event", typeName: "json", notNull: true, canInsertColumn: true},
		{name: "nullable_note", typeName: "text", canInsertColumn: true},
		{name: "defaulted_note", typeName: "text", notNull: true, defaultExpr: sqlNullString("'note'::text"), canInsertColumn: true},
		{name: "generated_note", typeName: "text", notNull: true, generated: "s", canInsertColumn: false},
	}}

	if err := validateDLQTableMetadata("dead_letters", metadata); err != nil {
		t.Fatalf("expected metadata to be valid, got %v", err)
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
		AddRow("poison", "sqs", "queue-a", "", nil).
		AddRow("confirmed", "sqs", "queue-a", "payload", nil)
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

	rows := mockEventRows().AddRow("poison", "pubsub", "bad/topic", "payload", nil)
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

	rows := mockEventRows().AddRow("poison", "sqs", "queue-a", "", nil)
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

func TestProcessOneBatchDeadLettersExpiredEvent(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.MaxEventAge = time.Minute
	cfg.DLQTable = "dead_letters"
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRowsWithTimestamp().AddRow("expired", "sqs", "queue-a", "payload", nil, time.Now().Add(-2*time.Minute))
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(insertDLQSQL).WithArgs(dlqPayloadMatcher{t: t, check: func(payload map[string]any) bool {
		if payload["target"] != "sqs" || payload["destination"] != "queue-a" {
			t.Errorf("unexpected resolved route in DLQ payload: %#v", payload)
			return false
		}
		errorText, _ := payload["error"].(string)
		if !strings.Contains(errorText, "expired") {
			t.Errorf("unexpected DLQ error: %#v", payload["error"])
			return false
		}
		return true
	}}).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(deleteOneSQL).WithArgs("expired").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected expired event not to be sent to SQS, got %#v", sqs.requests)
	}
	assertStatsSnapshot(t, a.stats.snapshotAndReset(), statsSnapshot{selected: 1, dlq: 1, batchesProcessed: 1})
}
