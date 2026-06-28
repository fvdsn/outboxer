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
	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

func TestProcessEventsStopsOnContextCancel(t *testing.T) {
	a := &app{cfg: testConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- a.processEvents(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processEvents returned an error after context cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("processEvents did not return after context cancellation")
	}
}

func TestProcessEventsStopsAfterFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}})

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", mockDBValue(combinedOrderingOptions()))...).
		AddRow(mockEventRow("event-2", "pubsub", "topic-1", "two", mockDBValue(combinedOrderingOptions()))...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	done := make(chan error, 1)
	go func() {
		done <- a.processEvents(context.Background())
	}()

	select {
	case err := <-done:
		if !errors.Is(err, errFatalAfterCommit) {
			t.Fatalf("processEvents returned %v, want fatal-after-commit error", err)
		}
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
	setTestPubSubProvider(a, &fakePubSubPublisher{errs: []error{nil, expectedErr}})

	firstRows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", nil)...).
		AddRow(mockEventRow("event-2", "pubsub", "topic-1", "two", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(firstRows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	secondErr := errors.New("second select failed")
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnError(secondErr)
	mock.ExpectRollback()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- a.processEvents(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processEvents returned an error after context cancellation: %v", err)
		}
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
	setTestPubSubProvider(a, &fakePubSubPublisher{errs: []error{nil, expectedErr}})

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", nil)...).
		AddRow(mockEventRow("event-2", "pubsub", "topic-1", "two", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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
	assertStatsSnapshot(t, a.stats.snapshotAndReset(), statsSnapshot{selected: 2, sent: 1, keptForRetry: 1, batchesProcessed: 1, senderErrors: 1})
}

func TestProcessOneBatchBeginFailureIsDatabaseError(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("begin failed")
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)
	mock.ExpectBegin().WillReturnError(expectedErr)

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database begin error, got %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected no selected events, got %d", result.selected)
	}
	assertStatsSnapshot(t, a.stats.snapshotAndReset(), statsSnapshot{batchErrors: 1})
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no sender calls after begin failure, got %#v", pubsub.messages)
	}
}

func TestProcessOneBatchEmptyBatchCommitsWithoutDelete(t *testing.T) {
	cfg := testConfig()
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, &fakePubSubPublisher{})
	setTestSQSProvider(a, &fakeSQSPublisher{autoReply: true})

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(mockEventRows())
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected empty batch, got %d selected events", result.selected)
	}
}

func TestSelectEventsQueryUsesBatchTargetAndBaseProjection(t *testing.T) {
	cfg := testConfig()
	cfg.CollectBatchTarget = 5000
	a := &app{cfg: cfg}

	query, args := a.selectEventsQuery()
	if !reflect.DeepEqual(args, []any{5000}) {
		t.Fatalf("collection query args = %#v, want %#v", args, []any{5000})
	}
	for _, expected := range []string{
		"WITH routes AS (",
		"count(*) OVER () AS route_count",
		"SELECT DISTINCT NULLIF(\"route_source\".\"target\", '') AS resolved_target",
		"CROSS JOIN LATERAL",
		"WHERE (NULLIF(\"candidate\".\"target\", '') IN ('pubsub', 'sqs'))",
		"LIMIT GREATEST(1, (($1::bigint + routes.route_count - 1) / routes.route_count))",
		"SELECT selected.resolved_target, selected.resolved_destination, \"events\".* FROM \"public\".\"events\" AS \"events\" JOIN selected",
		"ORDER BY \"events\".\"id\" FOR UPDATE",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected collection query to contain %q, got:\n%s", expected, query)
		}
	}
	if strings.Contains(query, "row_number()") {
		t.Fatalf("collection query should use route-local lateral scans, got:\n%s", query)
	}
}

func TestSelectEventsQuerySupportsMissingSingleBackendColumns(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.EventTarget = ""
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = "topic-default"
	a := &app{cfg: cfg}

	query, args := a.selectEventsQuery()
	if !reflect.DeepEqual(args, []any{cfg.CollectBatchTarget}) {
		t.Fatalf("collection query args = %#v, want %#v", args, []any{cfg.CollectBatchTarget})
	}
	for _, expected := range []string{
		"'pubsub' AS resolved_target",
		"'topic-default' AS resolved_destination",
		"FROM \"public\".\"events\" AS \"route_source\" WHERE (TRUE) AND COALESCE('topic-default', '') <> ''",
		"FROM \"public\".\"events\" AS \"candidate\" WHERE (TRUE) AND COALESCE('topic-default', '') <> ''",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected collection query to contain %q, got:\n%s", expected, query)
		}
	}
	if strings.Contains(query, "\"\"") {
		t.Fatalf("collection query must not reference empty optional column names, got:\n%s", query)
	}
}

func TestSelectEventsQuerySupportsMissingDestinationWithBackendDefaults(t *testing.T) {
	cfg := testConfig()
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = "topic-default"
	cfg.DefaultSQSQueueURL = "queue-default"
	a := &app{cfg: cfg}

	query, args := a.selectEventsQuery()
	if !reflect.DeepEqual(args, []any{cfg.CollectBatchTarget}) {
		t.Fatalf("collection query args = %#v, want %#v", args, []any{cfg.CollectBatchTarget})
	}
	for _, expected := range []string{
		"NULLIF(\"route_source\".\"target\", '') AS resolved_target",
		"CASE WHEN NULLIF(\"route_source\".\"target\", '') = 'pubsub' THEN 'topic-default' WHEN NULLIF(\"route_source\".\"target\", '') = 'sqs' THEN 'queue-default' ELSE '' END AS resolved_destination",
		"NULLIF(\"candidate\".\"target\", '') IN ('pubsub', 'sqs')",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected collection query to contain %q, got:\n%s", expected, query)
		}
	}
	if strings.Contains(query, "\"\"") {
		t.Fatalf("collection query must not reference empty optional column names, got:\n%s", query)
	}
}

func TestProviderRegistrationDrivesCollectionSQL(t *testing.T) {
	originalSpecs := providerSpecs
	providerSpecs = append(append([]providerSpec(nil), originalSpecs...), providerSpec{
		target:  "webhook",
		enabled: func(appConfig) bool { return true },
		route: func(appConfig) providerRoute {
			return providerRoute{
				target:             "webhook",
				defaultDestination: "https://example.com/events",
				ownedDestinations:  []string{"https://example.com/events"},
			}
		},
	})
	t.Cleanup(func() { providerSpecs = originalSpecs })

	query := (&app{cfg: testConfig()}).selectEventsQuerySQL()
	for _, expected := range []string{
		"IN ('pubsub', 'sqs', 'webhook')",
		"THEN 'https://example.com/events'",
		"<> 'webhook' OR",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected provider registration to add %q to collection query, got:\n%s", expected, query)
		}
	}
}

func TestRouteOwnershipSQLRestrictsConfiguredDestinations(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubDestinations = []string{"topic-a", "topic-b"}
	cfg.SQSDestinations = []string{"queue-a"}
	a := &app{cfg: cfg}

	got := a.routeOwnershipSQL("resolved_target", "resolved_destination")
	want := "(resolved_target <> 'pubsub' OR resolved_destination IN ('topic-a', 'topic-b')) AND (resolved_target <> 'sqs' OR resolved_destination IN ('queue-a'))"
	if got != want {
		t.Fatalf("unexpected ownership predicate:\ngot  %s\nwant %s", got, want)
	}
}

func TestSelectEventsQueryFiltersOwnedDestinations(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubDestinations = []string{"topic-a", "topic-b"}
	cfg.SQSDestinations = []string{"queue-a"}
	a := &app{cfg: cfg}

	query, args := a.selectEventsQuery()
	if !reflect.DeepEqual(args, []any{cfg.CollectBatchTarget}) {
		t.Fatalf("collection query args = %#v, want %#v", args, []any{cfg.CollectBatchTarget})
	}
	for _, expected := range []string{
		"\"route_source\".\"destination\", '')",
		"IN ('topic-a', 'topic-b')",
		"IN ('queue-a')",
		"\"candidate\".\"destination\", '')",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected collection query to contain %q, got:\n%s", expected, query)
		}
	}
}

func TestProcessOneBatchDispatchesByRegisteredTarget(t *testing.T) {
	cfg := testConfig()
	sender := &recordingProviderSender{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.senders = map[string]provider.Sender{"webhook": sender}

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "webhook", "https://example.com/events", "payload", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 || len(sender.events) != 1 {
		t.Fatalf("generic provider dispatch selected %d events and sent %d; want 1 and 1", result.selected, len(sender.events))
	}
	if sender.events[0].Destination != "https://example.com/events" {
		t.Fatalf("provider received destination %q", sender.events[0].Destination)
	}
}

func TestProcessOneBatchEmptySelectionCommitsWithoutRoutingLog(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

	query, _ := a.selectEventsQuery()
	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(cfg.CollectBatchTarget).WillReturnRows(mockEventRows())
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected no selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no Pub/Sub sends, got %#v", pubsub.messages)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no SQS sends, got %#v", sqs.requests)
	}
}

func TestProcessOneBatchCommitsHealthyRouteWhenAnotherRouteFails(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("sqs unavailable")
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{err: expectedErr}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

	query, _ := a.selectEventsQuery()
	rows := mockEventRows().
		AddRow(mockEventRow("pubsub-ok", "pubsub", "topic-a", "ok", nil)...).
		AddRow(mockEventRow("sqs-fail", "sqs", "queue-broken", "retry", nil)...)
	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(cfg.CollectBatchTarget).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("pubsub-ok").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected SQS sender error, got %v", err)
	}
	if errors.Is(err, errDatabaseBatch) {
		t.Fatalf("sender error should not be classified as database error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 1 || string(pubsub.messages[0].Data) != "ok" {
		t.Fatalf("expected healthy Pub/Sub route to publish once, got %#v", pubsub.messages)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected failing SQS route to be attempted once, got %#v", sqs.requests)
	}
}

func TestProcessOneBatchDeletesContentPoisonAndConfirmedSendTogether(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestSQSProvider(a, sqs)

	rows := mockEventRows().
		AddRow(mockEventRow("poison", "sqs", "queue-a", "", nil)...).
		AddRow(mockEventRow("confirmed", "sqs", "queue-a", "payload", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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

func TestProcessOneBatchDeletesExpiredEventWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.MaxEventAge = time.Minute
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestSQSProvider(a, sqs)

	rows := mockEventRowsWithTimestamp().
		AddRow(mockEventRow("expired", "sqs", "queue-a", "payload", nil, time.Now().Add(-2*time.Minute))...).
		AddRow(mockEventRow("fresh", "sqs", "queue-a", "payload", nil, time.Now())...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(deleteTwoSQL).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
	if len(sqs.requests) != 1 || len(sqs.requests[0].entries) != 1 || sqs.requests[0].entries[0].ID != "fresh" {
		t.Fatalf("expected only fresh event to be sent, got %#v", sqs.requests)
	}
	assertStatsSnapshot(t, a.stats.snapshotAndReset(), statsSnapshot{selected: 2, sent: 1, poison: 1, batchesProcessed: 1})
}

func TestExpiredEventBoundaryAndInvalidTimestamps(t *testing.T) {
	cfg := testConfig()
	cfg.MaxEventAge = time.Minute
	a := &app{cfg: cfg}
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		timestamp any
		want      bool
	}{
		{"older", now.Add(-time.Minute - time.Nanosecond), true},
		{"boundary", now.Add(-time.Minute), false},
		{"fresh", now.Add(-30 * time.Second), false},
		{"future", now.Add(time.Minute), false},
		{"nil", nil, false},
		{"bad", "not-a-time", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := event{columns: map[string]any{"timestamp": tt.timestamp}}
			if got := a.isExpiredEvent(evt, now); got != tt.want {
				t.Fatalf("isExpiredEvent() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestProcessOneBatchCommitsDoneBeforeFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}})

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", mockDBValue(combinedOrderingOptions()))...).
		AddRow(mockEventRow("event-2", "pubsub", "topic-1", "two", mockDBValue(combinedOrderingOptions()))...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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

func TestProcessOneBatchPreservesFatalAfterCommitOnCommitFailure(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("commit failed")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}})

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", mockDBValue(combinedOrderingOptions()))...).
		AddRow(mockEventRow("event-2", "pubsub", "topic-1", "two", mockDBValue(combinedOrderingOptions()))...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(expectedErr)

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, errFatalAfterCommit) || !errors.Is(err, errDatabaseBatch) || !errors.Is(err, expectedErr) {
		t.Fatalf("expected fatal-after-commit and database commit error, got %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
}

func TestProcessOneBatchPreservesFatalAfterCommitOnDeleteFailure(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("delete failed")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}})

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", mockDBValue(combinedOrderingOptions()))...).
		AddRow(mockEventRow("event-2", "pubsub", "topic-1", "two", mockDBValue(combinedOrderingOptions()))...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnError(expectedErr)
	mock.ExpectRollback()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, errFatalAfterCommit) || !errors.Is(err, errDatabaseBatch) || !errors.Is(err, expectedErr) {
		t.Fatalf("expected fatal-after-commit and database delete error, got %v", err)
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
	setTestPubSubProvider(a, &fakePubSubPublisher{})

	rows := mockEventRows().AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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
	setTestPubSubProvider(a, pubsub)

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnError(expectedErr)
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
	setTestPubSubProvider(a, &fakePubSubPublisher{})

	rows := mockEventRows().AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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
	setTestSQSProvider(a, sqs)

	rows := mockEventRows().AddRow(mockEventRow("event-1", "sqs", "queue-a", "one", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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
	setTestSQSProvider(a, sqs)

	rows := mockEventRows().AddRow(mockEventRow("event-1", "sqs", "queue-a", "one", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

	rows := mockEventRows().
		AddRow(mockEventRow("event-1", "pubsub", "topic-1", "one", nil)...).
		AddRow(mockEventRow("event-2", "sqs", "queue-a", "two", nil)...)
	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(rows)
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

func TestProcessOneBatchRoutesAndDeletesHundredMixedBackendEvents(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 16
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

	events := []event{}
	for i := 0; i < 50; i++ {
		events = append(events, testEvent(fmt.Sprintf("pubsub-%03d", i), "pubsub", "topic-a", "pubsub-payload"))
		events = append(events, testEvent(fmt.Sprintf("sqs-%03d", i), "sqs", "queue-a", "sqs-payload"))
	}

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(mockRowsForEvents(cfg, events))
	mock.ExpectExec(deleteEventsSQL(100)).WithArgs(anySQLArgs(100)...).WillReturnResult(sqlmock.NewResult(0, 100))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 100 {
		t.Fatalf("expected 100 selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 50 {
		t.Fatalf("expected 50 Pub/Sub messages, got %d", len(pubsub.messages))
	}
	if got := pubsubMessageCountByTopic(pubsub.messages)["topic-a"]; got != 50 {
		t.Fatalf("expected 50 Pub/Sub messages for topic-a, got %d", got)
	}
	if len(sqs.requests) != 5 {
		t.Fatalf("expected 5 SQS requests, got %d: %#v", len(sqs.requests), sqs.requests)
	}
	if got := sqsEntryCountByQueue(sqs.requests)["queue-a"]; got != 50 {
		t.Fatalf("expected 50 SQS entries for queue-a, got %d", got)
	}
}

func TestProcessOneBatchRoutesHundredMixedDestinations(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 16
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

	events := []event{}
	for i := 0; i < 50; i++ {
		events = append(events, testEvent(fmt.Sprintf("pubsub-%03d", i), "pubsub", fmt.Sprintf("topic-%d", i/10), "pubsub-payload"))
		events = append(events, testEvent(fmt.Sprintf("sqs-%03d", i), "sqs", fmt.Sprintf("queue-%d", i/10), "sqs-payload"))
	}

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(mockRowsForEvents(cfg, events))
	mock.ExpectExec(deleteEventsSQL(100)).WithArgs(anySQLArgs(100)...).WillReturnResult(sqlmock.NewResult(0, 100))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 100 {
		t.Fatalf("expected 100 selected events, got %d", result.selected)
	}
	for i := 0; i < 5; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		if got := pubsubMessageCountByTopic(pubsub.messages)[topic]; got != 10 {
			t.Fatalf("expected 10 messages for %s, got %d", topic, got)
		}
		queue := fmt.Sprintf("queue-%d", i)
		if got := sqsEntryCountByQueue(sqs.requests)[queue]; got != 10 {
			t.Fatalf("expected 10 entries for %s, got %d", queue, got)
		}
	}
}

func TestProcessOneBatchUsesSingleBackendDefaultTopicForHundredEvents(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.EventTarget = ""
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = "topic-default"
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)

	events := make([]event, 100)
	for i := range events {
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "", "", "payload")
	}

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(mockRowsForEvents(cfg, events))
	mock.ExpectExec(deleteEventsSQL(100)).WithArgs(anySQLArgs(100)...).WillReturnResult(sqlmock.NewResult(0, 100))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 100 {
		t.Fatalf("expected 100 selected events, got %d", result.selected)
	}
	if got := pubsubMessageCountByTopic(pubsub.messages)["topic-default"]; got != 100 {
		t.Fatalf("expected 100 messages for default topic, got %d", got)
	}
}

func TestProcessOneBatchUsesBackendDefaultsWithExplicitTargets(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultPubSubTopic = "topic-default"
	cfg.DefaultSQSQueueURL = "queue-default"
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

	events := []event{}
	for i := 0; i < 10; i++ {
		events = append(events, testEvent(fmt.Sprintf("pubsub-explicit-%03d", i), "pubsub", "topic-explicit", "payload"))
		events = append(events, testEvent(fmt.Sprintf("pubsub-default-%03d", i), "pubsub", "", "payload"))
		events = append(events, testEvent(fmt.Sprintf("sqs-explicit-%03d", i), "sqs", "queue-explicit", "payload"))
		events = append(events, testEvent(fmt.Sprintf("sqs-default-%03d", i), "sqs", "", "payload"))
	}

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(mockRowsForEvents(cfg, events))
	mock.ExpectExec(deleteEventsSQL(40)).WithArgs(anySQLArgs(40)...).WillReturnResult(sqlmock.NewResult(0, 40))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 40 {
		t.Fatalf("expected 40 selected events, got %d", result.selected)
	}
	if got := pubsubMessageCountByTopic(pubsub.messages); !reflect.DeepEqual(got, map[string]int{"topic-default": 10, "topic-explicit": 10}) {
		t.Fatalf("unexpected Pub/Sub topic counts: %#v", got)
	}
	if got := sqsEntryCountByQueue(sqs.requests); !reflect.DeepEqual(got, map[string]int{"queue-default": 10, "queue-explicit": 10}) {
		t.Fatalf("unexpected SQS queue counts: %#v", got)
	}
}

func TestProcessOneBatchProcessesBacklogAcrossMultipleSelectedBatches(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.SQSSendConcurrency = 16
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	setTestSQSProvider(a, sqs)

	for batchIndex, selected := range []int{100, 100, 50} {
		events := make([]event, selected)
		for i := range events {
			events[i] = testEvent(fmt.Sprintf("event-%d-%03d", batchIndex, i), "sqs", "queue-a", "payload")
		}
		mock.ExpectBegin()
		expectSelectEvents(mock, a).WillReturnRows(mockRowsForEvents(cfg, events))
		mock.ExpectExec(deleteEventsSQL(selected)).WithArgs(anySQLArgs(selected)...).WillReturnResult(sqlmock.NewResult(0, int64(selected)))
		mock.ExpectCommit()

		result, err := a.processOneBatch(context.Background())
		if err != nil {
			t.Fatalf("batch %d returned error: %v", batchIndex, err)
		}
		if result.selected != selected {
			t.Fatalf("batch %d selected %d events, want %d", batchIndex, result.selected, selected)
		}
	}

	mock.ExpectBegin()
	expectSelectEvents(mock, a).WillReturnRows(mockEventRows())
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("empty batch returned error: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected final empty batch, got %d selected events", result.selected)
	}
	if len(sqs.requests) != 25 {
		t.Fatalf("expected 25 SQS requests for 250 events, got %d: %#v", len(sqs.requests), sqs.requests)
	}
	if got := sqsEntryCountByQueue(sqs.requests)["queue-a"]; got != 250 {
		t.Fatalf("expected 250 SQS entries for queue-a, got %d", got)
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
			options jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	}()

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, timestamp, payload, target, destination, options)
		VALUES
			('pubsub-1', now(), 'hello pubsub', 'pubsub', 'topic-a', '{"pubsub":{"attributes":{"trace":"abc"}}}'),
			('sqs-1', now(), 'hello sqs', 'sqs', 'queue-a', '{"sqs":{"attributes":{"trace":{"DataType":"String","StringValue":"def"}}}}')
	`, ident(table)))
	if err != nil {
		t.Fatalf("insert events: %v", err)
	}

	cfg := testConfig()
	cfg.EventTable = table
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, db: db}
	setTestPubSubProvider(a, pubsub)
	setTestSQSProvider(a, sqs)

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

func TestPostgresIntegrationRouteSelectionAcrossAllRoutes(t *testing.T) {
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
			options jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	}()

	for i := 0; i < 100; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, timestamp, payload, target, destination)
			VALUES ($1, now(), 'payload', 'sqs', 'queue-a')
		`, ident(table)), fmt.Sprintf("000-route-a-%03d", i))
		if err != nil {
			t.Fatalf("insert route A event %d: %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, timestamp, payload, target, destination)
			VALUES ($1, now(), 'payload', 'pubsub', 'topic-b')
		`, ident(table)), fmt.Sprintf("900-route-b-%03d", i))
		if err != nil {
			t.Fatalf("insert route B event %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, timestamp, payload, target, destination)
			VALUES ($1, now(), 'payload', 'kafka', 'topic-c')
		`, ident(table)), fmt.Sprintf("999-unknown-%03d", i))
		if err != nil {
			t.Fatalf("insert unknown-target event %d: %v", i, err)
		}
	}

	cfg := testConfig()
	cfg.EventTable = table
	cfg.CollectBatchTarget = 40
	a := &app{cfg: cfg, db: db}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		t.Fatalf("select events: %v", err)
	}
	if len(events) != 30 {
		t.Fatalf("expected 30 selected events, got %d", len(events))
	}

	counts := map[string]int{}
	for _, evt := range events {
		counts[evt.route.destination]++
		target := eventString(evt, cfg.EventTarget)
		switch target {
		case eventTargetPubSub:
			if evt.route.target != eventTargetPubSub {
				t.Fatalf("Pub/Sub event has resolved target %q", evt.route.target)
			}
		case eventTargetSQS:
			if evt.route.target != eventTargetSQS {
				t.Fatalf("SQS event has resolved target %q", evt.route.target)
			}
		default:
			t.Fatalf("collection included unroutable target %q in event %#v", target, evt.columns)
		}
	}
	want := map[string]int{"queue-a": 20, "topic-b": 10}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("unexpected selected route counts: got %#v want %#v", counts, want)
	}
}

func TestPostgresIntegrationRouteGroupsExplicitAndDefaultDestinationTogether(t *testing.T) {
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
			options jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	}()

	for i := 0; i < 50; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, timestamp, payload, target, destination)
			VALUES ($1, now(), 'payload', 'pubsub', 'topic-default')
		`, ident(table)), fmt.Sprintf("000-explicit-%03d", i))
		if err != nil {
			t.Fatalf("insert explicit-destination event %d: %v", i, err)
		}
	}
	for i := 0; i < 50; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, timestamp, payload, target, destination)
			VALUES ($1, now(), 'payload', 'pubsub', '')
		`, ident(table)), fmt.Sprintf("100-default-%03d", i))
		if err != nil {
			t.Fatalf("insert default-destination event %d: %v", i, err)
		}
	}

	cfg := testConfig()
	cfg.EventTable = table
	cfg.SQSEnabled = false
	cfg.DefaultPubSubTopic = "topic-default"
	cfg.CollectBatchTarget = 40
	a := &app{cfg: cfg, db: db}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		t.Fatalf("select events: %v", err)
	}
	if len(events) != 40 {
		t.Fatalf("expected one resolved route capped at 40 events, got %d", len(events))
	}
	for _, evt := range events {
		if evt.route.target != eventTargetPubSub || evt.route.destination != "topic-default" {
			t.Fatalf("unexpected resolved route: %#v", evt.route)
		}
		if id := eventString(evt, cfg.EventID); !strings.HasPrefix(id, "000-explicit-") {
			t.Fatalf("expected selected events to be the oldest explicit/default shared route rows, got id %q", id)
		}
	}
}
