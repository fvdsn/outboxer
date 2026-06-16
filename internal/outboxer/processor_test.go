package outboxer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestResolveBackendRouting(t *testing.T) {
	bothEnabled := testConfig()

	pubsubOnly := testConfig()
	pubsubOnly.SQSEnabled = false

	sqsOnly := testConfig()
	sqsOnly.PubSubEnabled = false

	newEvent := func(target string) event {
		columns := map[string]any{"id": "event-1", "destination": "dest-1"}
		if target != "" {
			columns["target"] = target
		}
		return event{columns: columns}
	}

	cases := []struct {
		name   string
		cfg    appConfig
		target string
		want   backend
	}{
		{"both: explicit pubsub", bothEnabled, "pubsub", backendPubSub},
		{"both: explicit sqs", bothEnabled, "sqs", backendSQS},
		{"both: empty target is ambiguous", bothEnabled, "", backendNone},
		{"both: unknown target", bothEnabled, "kafka", backendNone},
		{"pubsub only: empty target routes to pubsub", pubsubOnly, "", backendPubSub},
		{"pubsub only: explicit sqs is unroutable", pubsubOnly, "sqs", backendNone},
		{"sqs only: empty target routes to sqs", sqsOnly, "", backendSQS},
		{"sqs only: explicit pubsub is unroutable", sqsOnly, "pubsub", backendNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &app{cfg: tc.cfg}
			if got := a.resolveBackend(newEvent(tc.target)); got != tc.want {
				t.Fatalf("resolveBackend(%q) = %v, want %v", tc.target, got, tc.want)
			}
		})
	}
}

func TestClassifyRouteFailures(t *testing.T) {
	bothEnabled := testConfig()

	pubsubOnly := testConfig()
	pubsubOnly.SQSEnabled = false

	sqsOnly := testConfig()
	sqsOnly.PubSubEnabled = false

	newEvent := func(target string, destination string) event {
		columns := map[string]any{"id": "event-1"}
		if target != "" {
			columns["target"] = target
		}
		if destination != "" {
			columns["destination"] = destination
		}
		return event{columns: columns}
	}

	cases := []struct {
		name        string
		cfg         appConfig
		evt         event
		wantBackend backend
		wantFailure routingFailure
	}{
		{"target pubsub enabled", bothEnabled, newEvent("pubsub", "topic-a"), backendPubSub, routingFailureNone},
		{"target sqs enabled", bothEnabled, newEvent("sqs", "queue-a"), backendSQS, routingFailureNone},
		{"target pubsub disabled", sqsOnly, newEvent("pubsub", ""), backendNone, routingFailureDisabled},
		{"target sqs disabled", pubsubOnly, newEvent("sqs", ""), backendNone, routingFailureDisabled},
		{"empty target one backend", pubsubOnly, newEvent("", "topic-a"), backendPubSub, routingFailureNone},
		{"empty target both backends", bothEnabled, newEvent("", "topic-a"), backendNone, routingFailureAmbiguous},
		{"unknown target", bothEnabled, newEvent("kafka", "topic-a"), backendNone, routingFailureUnsupported},
		{"empty destination no default", pubsubOnlyNoDefault(), newEvent("pubsub", ""), backendNone, routingFailureNoDestination},
		{"disabled backend before destination", pubsubOnly, newEvent("sqs", ""), backendNone, routingFailureDisabled},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &app{cfg: tc.cfg}
			got := a.classifyRoute(tc.evt)
			if got.backend != tc.wantBackend || got.failure != tc.wantFailure {
				t.Fatalf("classifyRoute() = {backend:%v failure:%q}, want {backend:%v failure:%q}", got.backend, got.failure, tc.wantBackend, tc.wantFailure)
			}
		})
	}
}

func TestProcessEventsStopsOnContextCancel(t *testing.T) {
	a := &app{cfg: testConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		a.processEvents(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processEvents did not return after context cancellation")
	}
}

func TestProcessEventsStopsAfterFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}}

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", "key-a", nil).
		AddRow("event-2", "pubsub", "topic-1", "two", "key-a", nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	done := make(chan struct{})
	go func() {
		a.processEvents(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processEvents did not stop after fatal-after-commit error")
	}
}

func TestProcessEventsDoesNotCooldownAfterNonFatalSenderError(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.ErrorCooldown = time.Hour
	expectedErr := errors.New("retryable pubsub")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, expectedErr}}

	firstRows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", nil, nil).
		AddRow("event-2", "pubsub", "topic-1", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(firstRows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	secondErr := errors.New("second select failed")
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnError(secondErr)
	mock.ExpectRollback()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.processEvents(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processEvents did not return after cancellation")
	}
}

func TestProcessOneBatchCommitsDoneBeforeNonFatalSenderError(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("retryable pubsub")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, expectedErr}}

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", nil, nil).
		AddRow("event-2", "pubsub", "topic-1", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected non-fatal sender error, got %v", err)
	}
	if errors.Is(err, errDatabaseBatch) {
		t.Fatalf("sender error should not be classified as database error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
}

func TestProcessOneBatchBeginFailureIsDatabaseError(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("begin failed")
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub
	mock.ExpectBegin().WillReturnError(expectedErr)

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database begin error, got %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected no selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no sender calls after begin failure, got %#v", pubsub.messages)
	}
}

func TestProcessOneBatchEmptyBatchCommitsWithoutDelete(t *testing.T) {
	cfg := testConfig()
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{}
	a.sqs = &fakeSQSPublisher{autoReply: true}

	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(mockEventRows())
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected empty batch, got %d selected events", result.selected)
	}
}

func TestProcessOneBatchDeletesContentPoisonAndConfirmedSendTogether(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("poison", "sqs", "queue-a", "", nil, nil).
		AddRow("confirmed", "sqs", "queue-a", "payload", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
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

func TestProcessOneBatchCommitsDoneBeforeFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}}

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", "key-a", nil).
		AddRow("event-2", "pubsub", "topic-1", "two", "key-a", nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, errFatalAfterCommit) {
		t.Fatalf("expected fatal-after-commit error, got %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
}

func TestProcessOneBatchRollsBackOnDeleteFailure(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("delete failed")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{}

	rows := mockEventRows().AddRow("event-1", "pubsub", "topic-1", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnError(expectedErr)
	mock.ExpectRollback()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database delete error, got %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchRollsBackOnSelectFailure(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("select failed")
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub

	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnError(expectedErr)
	mock.ExpectRollback()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database select error, got %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected no selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no sender calls after select failure, got %#v", pubsub.messages)
	}
}

func TestProcessOneBatchCommitFailureIsDatabaseError(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("commit failed")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{}

	rows := mockEventRows().AddRow("event-1", "pubsub", "topic-1", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(expectedErr)

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database commit error, got %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchRoutingFailuresOnlyCommitWithoutSendOrDelete(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("event-1", "kafka", "topic-1", "one", nil, nil).
		AddRow("event-2", "", "topic-2", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no Pub/Sub sends for routing failures, got %#v", pubsub.messages)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no SQS sends for routing failures, got %#v", sqs.requests)
	}
}

func TestProcessOneBatchDeduplicatesDoneIDs(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{response: sqsBatchResponse{
		Successful: []sqsBatchSuccess{
			{ID: "event-1", MessageID: "message-1"},
			{ID: "event-1", MessageID: "message-1-duplicate"},
		},
	}}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().AddRow("event-1", "sqs", "queue-a", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchIgnoresDoneIDOutsideSelectedBatch(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{response: sqsBatchResponse{
		Successful: []sqsBatchSuccess{{ID: "unknown-entry", MessageID: "message-unknown"}},
	}}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().AddRow("event-1", "sqs", "queue-a", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchRunsEnabledBackendsConcurrently(t *testing.T) {
	cfg := testConfig()
	pubsub := &trackingPubSubPublisher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	sqs := &trackingSQSPublisher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", nil, nil).
		AddRow("event-2", "sqs", "queue-a", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteTwoSQL).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	done := make(chan error, 1)
	go func() {
		_, err := a.processOneBatch(context.Background())
		done <- err
	}()

	for name, started := range map[string]chan struct{}{
		"pubsub": pubsub.started,
		"sqs":    sqs.started,
	} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("%s backend did not start before release", name)
		}
	}

	pubsub.release <- struct{}{}
	sqs.release <- struct{}{}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processOneBatch returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("processOneBatch did not finish after releasing backends")
	}
}

func TestPostgresIntegrationProcessesAndDeletesEvents(t *testing.T) {
	dsn := os.Getenv("OUTBOXER_INTEGRATION_PG_DSN")
	if dsn == "" {
		t.Skip("set OUTBOXER_INTEGRATION_PG_DSN to run the Postgres integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	table := "outboxer_test_" + strings.ReplaceAll(strconvNano(), "-", "_")
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id text PRIMARY KEY,
			timestamp timestamptz,
			payload text NOT NULL,
			target text,
			destination text,
			ordering_key text,
			attributes jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	}()

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, timestamp, payload, target, destination, ordering_key, attributes)
		VALUES
			('pubsub-1', now(), 'hello pubsub', 'pubsub', 'topic-a', null, '{"trace":"abc"}'),
			('sqs-1', now(), 'hello sqs', 'sqs', 'queue-a', null, '{"trace":"def"}')
	`, ident(table)))
	if err != nil {
		t.Fatalf("insert events: %v", err)
	}

	cfg := testConfig()
	cfg.EventTable = table
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, db: db, pubsub: pubsub, sqs: sqs}

	result, err := a.processOneBatch(ctx)
	if err != nil {
		t.Fatalf("process events: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected 2 selected events, got %d", result.selected)
	}

	var remaining int
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", ident(table))).Scan(&remaining); err != nil {
		t.Fatalf("count remaining events: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected all events deleted, got %d remaining", remaining)
	}

	if len(pubsub.messages) != 1 {
		t.Fatalf("expected one pubsub message, got %d", len(pubsub.messages))
	}
	if len(sqs.requests) != 1 || len(sqs.requests[0].entries) != 1 {
		t.Fatalf("expected one sqs message, got %#v", sqs.requests)
	}

	gotBodies := []string{string(pubsub.messages[0].Data), sqs.requests[0].entries[0].MessageBody}
	sort.Strings(gotBodies)
	if !reflect.DeepEqual(gotBodies, []string{"hello pubsub", "hello sqs"}) {
		t.Fatalf("unexpected published bodies: %#v", gotBodies)
	}

	result, err = a.processOneBatch(ctx)
	if err != nil {
		t.Fatalf("process empty batch: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected empty batch to select 0 events, got %d", result.selected)
	}
}
