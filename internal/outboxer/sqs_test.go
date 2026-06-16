package outboxer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSendSQSEventsUsesDefaultQueueURL(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.DefaultSQSQueueURL = "https://sqs.example/default"
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "payload": "one"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) { deleted = append(deleted, id) }); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 1 || sqs.requests[0].queueURL != "https://sqs.example/default" {
		t.Fatalf("expected request to default queue URL, got %#v", sqs.requests)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQS10EventsRespectsPublishTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 50 * time.Millisecond
	a := &app{cfg: cfg, sqs: blockingSQSPublisher{}}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "p"}},
	}

	start := time.Now()
	err := a.sendSQS10Events(context.Background(), "queue-a", events, func(any) {})
	if err == nil {
		t.Fatal("expected a timeout error from a blocked SendBatch")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SendBatch blocked for %s instead of timing out", elapsed)
	}
}

func TestSQSLocalPrevalidationBoundaries(t *testing.T) {
	attrs := map[string]string{}
	for i := 0; i < sqsMaxAttributes; i++ {
		attrs[fmt.Sprintf("attr%d", i)] = "value"
	}
	if !validSQSAttributes(attrs) {
		t.Fatal("expected exactly max SQS attributes to be valid")
	}
	attrs["overflow"] = "value"
	if validSQSAttributes(attrs) {
		t.Fatal("expected too many SQS attributes to be invalid")
	}

	invalidAttrs := []map[string]string{
		{"": "value"},
		{".bad": "value"},
		{"bad.": "value"},
		{"bad..name": "value"},
		{"AWS.trace": "value"},
		{"Amazon.trace": "value"},
		{"bad name": "value"},
		{strings.Repeat("k", 257): "value"},
		{"empty": ""},
	}
	for _, attr := range invalidAttrs {
		if validSQSAttributes(attr) {
			t.Fatalf("expected SQS attributes %#v to be invalid", attr)
		}
	}

	if isSQSPoison([]byte("body"), nil, false, "") {
		t.Fatal("expected ordinary SQS body to be valid")
	}
	if !isSQSPoison(nil, nil, false, "") {
		t.Fatal("expected empty SQS body to be poison")
	}
	if !isSQSPoison([]byte{0xff}, nil, false, "") {
		t.Fatal("expected invalid UTF-8 SQS body to be poison")
	}
	if !isSQSPoison([]byte("body"), map[string]string{"attr": string([]byte{0xff})}, false, "") {
		t.Fatal("expected invalid UTF-8 SQS attribute value to be poison")
	}
	if isSQSPoison([]byte("body\t\n\r"), nil, false, "") {
		t.Fatal("expected allowed SQS boundary characters to be valid")
	}
	if !isSQSPoison([]byte(strings.Repeat("x", sqsEventMaxSizeByte+1)), nil, false, "") {
		t.Fatal("expected oversized SQS message to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, true, strings.Repeat("x", 129)) {
		t.Fatal("expected overlong FIFO group id to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, true, "bad\nkey") {
		t.Fatal("expected invalid FIFO group id to be poison")
	}
}

func TestSendSQS10EventsHandlesStandardPartialResponses(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{response: sqsBatchResponse{
		Successful: []sqsBatchSuccess{{ID: "event-1", MessageID: "message-1"}},
		Failed: []sqsBatchFailure{
			{ID: "event-2", Code: "InvalidMessageContents", Message: "bad", SenderFault: true},
			{ID: "event-3", Code: "InternalError", Message: "later", SenderFault: false},
		},
	}}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "attributes": []byte(`{"ok":"1","bad":true}`)}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": "two"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a", "payload": "three"}},
	}

	err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected one SQS request, got %d", len(sqs.requests))
	}
	if !reflect.DeepEqual(sqs.requests[0].entries[0].Attributes, map[string]string{"ok": "1"}) {
		t.Fatalf("unexpected sanitized attributes: %#v", sqs.requests[0].entries[0].Attributes)
	}
}

func TestSendSQS10EventsIsolatesPermanentBatchRequestError(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{
		errs: []error{fakeSQSAPIError{code: "InvalidMessageContents"}, nil, nil},
		responses: []sqsBatchResponse{
			{Successful: []sqsBatchSuccess{{ID: "event-1", MessageID: "message-1"}}},
			{Successful: []sqsBatchSuccess{{ID: "event-2", MessageID: "message-2"}}},
		},
	}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": "two"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 3 {
		t.Fatalf("expected original batch plus two isolated sends, got %#v", sqs.requests)
	}
	if len(sqs.requests[0].entries) != 2 || len(sqs.requests[1].entries) != 1 || len(sqs.requests[2].entries) != 1 {
		t.Fatalf("unexpected isolation request shapes: %#v", sqs.requests)
	}
}

func TestSendSQS10EventsRetryableRequestErrorKeepsEvents(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("temporary SQS outage")
	sqs := &fakeSQSPublisher{err: expectedErr}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one"}},
	}

	err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected retryable SQS error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQS10EventsCanceledContextKeepsEvents(t *testing.T) {
	cfg := testConfig()
	sqs := &recordingBlockingSQSPublisher{}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "payload"}},
	}

	err := a.sendSQS10Events(ctx, "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQSEventsStandardUsesConcurrencyLimit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 2
	sqs := &trackingSQSPublisher{
		started: make(chan struct{}, 3),
		release: make(chan struct{}, 3),
	}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 25)
	for i := range events {
		events[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("event-%02d", i),
			"destination": "queue-a",
			"payload":     "payload",
		}}
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	done := make(chan error, 1)
	go func() {
		done <- a.sendSQSEvents(context.Background(), events, func(id any) {
			deletedMu.Lock()
			defer deletedMu.Unlock()
			deleted = append(deleted, id)
		})
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-sqs.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial SQS requests")
		}
	}
	select {
	case <-sqs.started:
		t.Fatal("third SQS request started before concurrency slot was released")
	case <-time.After(50 * time.Millisecond):
	}

	sqs.release <- struct{}{}
	sqs.release <- struct{}{}
	select {
	case <-sqs.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for final SQS request")
	}
	sqs.release <- struct{}{}

	if err := <-done; err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	sqs.mu.Lock()
	maxInFlight := sqs.max
	requestCount := len(sqs.requests)
	sqs.mu.Unlock()
	if maxInFlight > 2 {
		t.Fatalf("expected at most 2 concurrent SQS requests, got %d", maxInFlight)
	}
	if requestCount != 3 {
		t.Fatalf("expected 3 chunked SQS requests, got %d", requestCount)
	}
	deletedMu.Lock()
	deletedCount := len(deleted)
	deletedMu.Unlock()
	if deletedCount != len(events) {
		t.Fatalf("expected all events deleted, got %d", deletedCount)
	}
}

func TestSendSQSEventsStandardSplitsByBatchSize(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": strings.Repeat("a", 600*1024)}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": strings.Repeat("b", 600*1024)}},
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 2 {
		t.Fatalf("expected size split into two SQS requests, got %#v", sqs.requests)
	}
	for _, request := range sqs.requests {
		if len(request.entries) != 1 {
			t.Fatalf("expected one entry per size-split request, got %#v", request.entries)
		}
	}
	if len(deleted) != 2 {
		t.Fatalf("expected both events deleted, got %#v", deleted)
	}
}

func TestSendSQSEventsStandardSplitsByCount(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 11)
	for i := range events {
		events[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("event-%02d", i),
			"destination": "queue-a",
			"payload":     "payload",
		}}
	}

	if err := a.sendSQSEvents(context.Background(), events, func(any) {}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 2 {
		t.Fatalf("expected 11 events to split into two requests, got %#v", sqs.requests)
	}
	sizes := []int{len(sqs.requests[0].entries), len(sqs.requests[1].entries)}
	sort.Ints(sizes)
	if !reflect.DeepEqual(sizes, []int{1, 10}) {
		t.Fatalf("unexpected chunk sizes: %#v", sizes)
	}
}

func TestSendSQSEventsStandardSendsTenInOneBatch(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 10)
	for i := range events {
		events[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("event-%02d", i),
			"destination": "queue-a",
			"payload":     "payload",
		}}
	}

	if err := a.sendSQSEvents(context.Background(), events, func(any) {}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected one SQS request, got %#v", sqs.requests)
	}
	if len(sqs.requests[0].entries) != 10 {
		t.Fatalf("expected ten SQS entries, got %#v", sqs.requests[0].entries)
	}
}

func TestSendSQSEventsFIFOStopsGroupAfterRetryableFailure(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{responses: []sqsBatchResponse{
		{Successful: []sqsBatchSuccess{{ID: "event-1", MessageID: "message-1"}}},
		{Failed: []sqsBatchFailure{{ID: "event-2", Code: "InternalError", Message: "retry", SenderFault: false}}},
	}}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "ordering_key": "group-a"}},
	}

	err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 2 {
		t.Fatalf("expected two single-message FIFO requests, got %#v", sqs.requests)
	}
	for _, request := range sqs.requests {
		if len(request.entries) != 1 {
			t.Fatalf("expected one FIFO entry per request, got %#v", request.entries)
		}
		entry := request.entries[0]
		if entry.MessageGroupID != "group-a" {
			t.Fatalf("expected FIFO group id group-a, got %q", entry.MessageGroupID)
		}
		if entry.DeduplicationID != entry.ID {
			t.Fatalf("expected raw valid event id as dedup id, got %q for %q", entry.DeduplicationID, entry.ID)
		}
	}
}

func TestSendSQSEventsFIFOOneGroupAllSuccessUsesSingleMessageRequests(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "ordering_key": "group-a"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2", "event-3"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 3 {
		t.Fatalf("expected three single-message FIFO requests, got %#v", sqs.requests)
	}
	for _, request := range sqs.requests {
		if len(request.entries) != 1 {
			t.Fatalf("expected one entry per FIFO request, got %#v", request.entries)
		}
		if request.entries[0].MessageGroupID != "group-a" {
			t.Fatalf("unexpected message group ID: %q", request.entries[0].MessageGroupID)
		}
	}
}

func TestSendSQSEventsFIFOProcessesDifferentGroups(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deletedMu sync.Mutex
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-b"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	deletedMu.Lock()
	deletedCopy := append([]any(nil), deleted...)
	deletedMu.Unlock()
	sort.Slice(deletedCopy, func(i, j int) bool { return fmt.Sprint(deletedCopy[i]) < fmt.Sprint(deletedCopy[j]) })
	if !reflect.DeepEqual(deletedCopy, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deletedCopy)
	}
	if len(sqs.requests) != 2 {
		t.Fatalf("expected one request per FIFO group event, got %#v", sqs.requests)
	}
	groups := []string{sqs.requests[0].entries[0].MessageGroupID, sqs.requests[1].entries[0].MessageGroupID}
	sort.Strings(groups)
	if !reflect.DeepEqual(groups, []string{"group-a", "group-b"}) {
		t.Fatalf("unexpected FIFO groups: %#v", groups)
	}
}

func TestSendSQSEventsFIFOAppliesGroupBatchCap(t *testing.T) {
	cfg := testConfig()
	cfg.OrderedGroupBatchCap = 2
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "ordering_key": "group-a"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 2 {
		t.Fatalf("expected cap to send two requests, got %#v", sqs.requests)
	}
}

func TestSendSQSEventsFIFODifferentGroupCanSucceedWhenOneFails(t *testing.T) {
	cfg := testConfig()
	sqs := &keyedSQSPublisher{responses: map[string]sqsBatchResponse{
		"event-1": {Failed: []sqsBatchFailure{{ID: "event-1", Code: "InternalError", Message: "retry", SenderFault: false}}},
		"event-2": {Successful: []sqsBatchSuccess{{ID: "event-2", MessageID: "message-2"}}},
	}}
	a := &app{cfg: cfg, sqs: sqs}
	var deletedMu sync.Mutex
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-b"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	deletedMu.Lock()
	defer deletedMu.Unlock()
	if !reflect.DeepEqual(deleted, []any{"event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQSEventsFIFOContinuesAfterContentPoison(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected only the non-poison event to reach SQS, got %#v", sqs.requests)
	}
	if got := sqs.requests[0].entries[0].ID; got != "event-2" {
		t.Fatalf("expected event-2 to be sent after poison event, got %q", got)
	}
}

func TestSendSQSEventsFIFOTimeoutStopsSameGroup(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	sqs := &recordingBlockingSQSPublisher{}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
	}

	err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	sqs.mu.Lock()
	requests := append([]fakeSQSRequest(nil), sqs.requests...)
	sqs.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("expected only first same-group event to be sent, got %#v", requests)
	}
	if got := requests[0].entries[0].ID; got != "event-1" {
		t.Fatalf("expected event-1 to be the only attempted send, got %q", got)
	}
}

func TestSendSQS10EventsStandardQueueOmitsFIFOFields(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	// A standard queue must not get a group id even when an ordering key is set.
	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "ordering_key": "group-a"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.MessageGroupID != "" {
		t.Fatalf("expected no message group id on standard queue, got %q", entry.MessageGroupID)
	}
	if entry.DeduplicationID != "" {
		t.Fatalf("expected no dedup id on standard queue, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsFIFOWithoutOrderingKeyGetsFallbackGroup(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.MessageGroupID == "" {
		t.Fatal("expected a fallback message group id on FIFO queue")
	}
	if entry.MessageGroupID != syntheticFIFOGroupID("event-1") {
		t.Fatalf("expected stable synthetic group id, got %q", entry.MessageGroupID)
	}
	if entry.DeduplicationID != "event-1" {
		t.Fatalf("expected dedup id to equal event id, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsFIFODerivesSafeDedupID(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	rawID := strings.Repeat("x", 129)

	events := []event{
		{columns: map[string]any{"id": rawID, "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.ID == rawID {
		t.Fatal("expected oversized raw event id to be replaced for batch entry id")
	}
	if entry.DeduplicationID == rawID {
		t.Fatal("expected oversized raw event id to be replaced for dedup id")
	}
	if len(entry.DeduplicationID) != 64 {
		t.Fatalf("expected SHA-256 hex dedup id, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsStandardDerivesSafeBatchEntryID(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	rawID := strings.Repeat("x", 81)

	events := []event{
		{columns: map[string]any{"id": rawID, "destination": "queue-a", "payload": "one"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.ID == rawID {
		t.Fatal("expected oversized raw event id to be replaced for standard batch entry id")
	}
	if len(entry.ID) != 64 {
		t.Fatalf("expected SHA-256 hex entry id, got %q", entry.ID)
	}
}

func TestSendSQS10EventsDropsLocalPoisonWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "empty", "destination": "queue-a", "payload": ""}},
		{columns: map[string]any{"id": "large", "destination": "queue-a", "payload": strings.Repeat("x", sqsEventMaxSizeByte+1)}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"empty", "large"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no provider calls for local poison, got %#v", sqs.requests)
	}
}

func TestSendSQS10EventsDropsInvalidQueueURLWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "bad queue url", "payload": "payload"}},
	}

	if err := a.sendSQS10Events(context.Background(), "bad queue url", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no provider calls for invalid queue URL, got %#v", sqs.requests)
	}
}

func TestSendSQS10EventsKeepsSyntacticallyValidMissingQueue(t *testing.T) {
	cfg := testConfig()
	expectedErr := fakeSQSAPIError{code: "QueueDoesNotExist"}
	sqs := &fakeSQSPublisher{err: expectedErr}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "https://sqs.us-east-1.amazonaws.com/123456789012/missing", "payload": "payload"}},
	}

	err := a.sendSQS10Events(context.Background(), "https://sqs.us-east-1.amazonaws.com/123456789012/missing", events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected missing queue error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("expected missing queue event to be kept, got deleted ids %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected provider call for syntactically valid queue URL, got %#v", sqs.requests)
	}
}

func TestSendSQS10EventsKeepsRetryableProviderErrors(t *testing.T) {
	for _, code := range []string{"QueueDoesNotExist", "AccessDenied", "ExpiredToken", "ThrottlingException"} {
		t.Run(code, func(t *testing.T) {
			cfg := testConfig()
			expectedErr := fakeSQSAPIError{code: code}
			sqs := &fakeSQSPublisher{err: expectedErr}
			a := &app{cfg: cfg, sqs: sqs}
			var deleted []any

			events := []event{
				{columns: map[string]any{"id": "event-1", "destination": "https://sqs.us-east-1.amazonaws.com/123456789012/queue", "payload": "payload"}},
			}

			err := a.sendSQS10Events(context.Background(), "https://sqs.us-east-1.amazonaws.com/123456789012/queue", events, func(id any) {
				deleted = append(deleted, id)
			})
			if !errors.Is(err, expectedErr) {
				t.Fatalf("expected provider error, got %v", err)
			}
			if len(deleted) != 0 {
				t.Fatalf("expected event to be kept for %s, got deleted ids %#v", code, deleted)
			}
		})
	}
}
