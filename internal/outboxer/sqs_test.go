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

func sqsStringAttribute(value string) sqsMessageAttribute {
	return sqsMessageAttribute{DataType: "String", StringValue: value, HasStringValue: true}
}

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

	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) { deleted = append(deleted, id) }); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 1 || sqs.requests[0].queueURL != "https://sqs.example/default" {
		t.Fatalf("expected request to default queue URL, got %#v", sqs.requests)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQSEventsStandardSendsHundredAsTenBatches(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 16
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 100)
	for i := range events {
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "sqs", "queue-a", "payload", "")
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 10 {
		t.Fatalf("expected 10 SQS requests, got %d: %#v", len(sqs.requests), sqs.requests)
	}
	for _, request := range sqs.requests {
		if request.queueURL != "queue-a" {
			t.Fatalf("expected queue-a request, got %q", request.queueURL)
		}
		if len(request.entries) != 10 {
			t.Fatalf("expected 10 entries per request, got %d in %#v", len(request.entries), request)
		}
		for _, entry := range request.entries {
			if entry.MessageGroupID != "" || entry.DeduplicationID != "" {
				t.Fatalf("standard queue entry should not include FIFO fields: %#v", entry)
			}
		}
	}
	deletedMu.Lock()
	gotDeleted := sortedDeletedIDs(deleted)
	deletedMu.Unlock()
	if got, want := gotDeleted, expectedHundredEventIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deleted IDs:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestSendSQSEventsStandardGroupsHundredAcrossTenQueues(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 16
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 100)
	for i := range events {
		queue := fmt.Sprintf("queue-%02d", i/10)
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "sqs", queue, "payload", "")
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 10 {
		t.Fatalf("expected one request per queue, got %d: %#v", len(sqs.requests), sqs.requests)
	}
	for i := 0; i < 10; i++ {
		queue := fmt.Sprintf("queue-%02d", i)
		if got := sqsRequestCountByQueue(sqs.requests)[queue]; got != 1 {
			t.Fatalf("expected one request for %s, got %d in %#v", queue, got, sqs.requests)
		}
		if got := sqsEntryCountByQueue(sqs.requests)[queue]; got != 10 {
			t.Fatalf("expected 10 entries for %s, got %d in %#v", queue, got, sqs.requests)
		}
	}
	deletedMu.Lock()
	gotDeleted := sortedDeletedIDs(deleted)
	deletedMu.Unlock()
	if got, want := gotDeleted, expectedHundredEventIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deleted IDs:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestSendSQSEventsUsesDefaultQueueForHundredEvents(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.EventTarget = ""
	cfg.EventDestination = ""
	cfg.DefaultSQSQueueURL = "https://sqs.example/default"
	cfg.SQSSendConcurrency = 16
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 100)
	for i := range events {
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "", "", "payload", "")
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 10 {
		t.Fatalf("expected 10 requests to default queue, got %d: %#v", len(sqs.requests), sqs.requests)
	}
	if got := sqsEntryCountByQueue(sqs.requests)["https://sqs.example/default"]; got != 100 {
		t.Fatalf("expected all events to use default queue, got %d in %#v", got, sqs.requests)
	}
	deletedMu.Lock()
	gotDeleted := sortedDeletedIDs(deleted)
	deletedMu.Unlock()
	if got, want := gotDeleted, expectedHundredEventIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deleted IDs:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestSendSQSEventsMixedStandardAndFIFOHappyPath(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 16
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{}
	for i := 0; i < 20; i++ {
		events = append(events, testEvent(fmt.Sprintf("standard-%03d", i), "sqs", "standard-queue", "payload", ""))
	}
	for i := 0; i < 6; i++ {
		group := fmt.Sprintf("group-%d", i%2)
		events = append(events, testEvent(fmt.Sprintf("fifo-%03d", i), "sqs", "orders.fifo", "payload", group))
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if got := sqsRequestCountByQueue(sqs.requests)["standard-queue"]; got != 2 {
		t.Fatalf("expected 2 standard requests, got %d in %#v", got, sqs.requests)
	}
	if got := sqsEntryCountByQueue(sqs.requests)["standard-queue"]; got != 20 {
		t.Fatalf("expected 20 standard entries, got %d in %#v", got, sqs.requests)
	}
	if got := sqsRequestCountByQueue(sqs.requests)["orders.fifo"]; got != 6 {
		t.Fatalf("expected one FIFO request per event, got %d in %#v", got, sqs.requests)
	}
	for _, request := range sqs.requests {
		if request.queueURL != "orders.fifo" {
			continue
		}
		if len(request.entries) != 1 {
			t.Fatalf("expected single-entry FIFO request, got %#v", request)
		}
		if request.entries[0].MessageGroupID == "" || request.entries[0].DeduplicationID == "" {
			t.Fatalf("expected FIFO fields on entry: %#v", request.entries[0])
		}
	}
	deletedMu.Lock()
	deletedCount := len(deleted)
	deletedMu.Unlock()
	if deletedCount != len(events) {
		t.Fatalf("expected all events deleted, got %d of %d: %#v", deletedCount, len(events), deleted)
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
	err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(any) {})
	if err == nil {
		t.Fatal("expected a timeout error from a blocked SendBatch")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SendBatch blocked for %s instead of timing out", elapsed)
	}
}

func TestSQSLocalPrevalidationBoundaries(t *testing.T) {
	attrs := map[string]sqsMessageAttribute{}
	for i := 0; i < sqsMaxAttributes; i++ {
		attrs[fmt.Sprintf("attr%d", i)] = sqsStringAttribute("value")
	}
	if !validSQSAttributes(attrs) {
		t.Fatal("expected exactly max SQS attributes to be valid")
	}
	attrs["overflow"] = sqsStringAttribute("value")
	if validSQSAttributes(attrs) {
		t.Fatal("expected too many SQS attributes to be invalid")
	}

	invalidAttrs := []map[string]sqsMessageAttribute{
		{"": sqsStringAttribute("value")},
		{".bad": sqsStringAttribute("value")},
		{"bad.": sqsStringAttribute("value")},
		{"bad..name": sqsStringAttribute("value")},
		{"AWS.trace": sqsStringAttribute("value")},
		{"Amazon.trace": sqsStringAttribute("value")},
		{"bad name": sqsStringAttribute("value")},
		{strings.Repeat("k", 257): sqsStringAttribute("value")},
		{"empty": sqsStringAttribute("")},
		{"missingType": {HasStringValue: true, StringValue: "value"}},
		{"binaryWithString": {DataType: "Binary", HasStringValue: true, StringValue: "value"}},
		{"stringWithBinary": {DataType: "String", HasBinaryValue: true, BinaryValue: []byte("value")}},
	}
	for _, attr := range invalidAttrs {
		if validSQSAttributes(attr) {
			t.Fatalf("expected SQS attributes %#v to be invalid", attr)
		}
	}

	if isSQSPoison([]byte("body"), nil, "", "", nil) {
		t.Fatal("expected ordinary SQS body to be valid")
	}
	if !isSQSPoison(nil, nil, "", "", nil) {
		t.Fatal("expected empty SQS body to be poison")
	}
	if !isSQSPoison([]byte{0xff}, nil, "", "", nil) {
		t.Fatal("expected invalid UTF-8 SQS body to be poison")
	}
	if !isSQSPoison([]byte("body"), map[string]sqsMessageAttribute{"attr": sqsStringAttribute(string([]byte{0xff}))}, "", "", nil) {
		t.Fatal("expected invalid UTF-8 SQS attribute value to be poison")
	}
	if isSQSPoison([]byte("body\t\n\r"), nil, "", "", nil) {
		t.Fatal("expected allowed SQS boundary characters to be valid")
	}
	if !isSQSPoison([]byte(strings.Repeat("x", sqsEventMaxSizeByte+1)), nil, "", "", nil) {
		t.Fatal("expected oversized SQS message to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, strings.Repeat("x", 129), "", nil) {
		t.Fatal("expected overlong FIFO group id to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, "bad\nkey", "", nil) {
		t.Fatal("expected invalid FIFO group id to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, "bad\nkey", "", nil) {
		t.Fatal("expected invalid standard fair queue group id to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, "", strings.Repeat("x", 129), nil) {
		t.Fatal("expected overlong deduplication id to be poison")
	}
	invalidDelay := int32(901)
	if !isSQSPoison([]byte("body"), nil, "", "", &invalidDelay) {
		t.Fatal("expected invalid delay to be poison")
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "options": []byte(`{"sqs":{"attributes":{"ok":{"DataType":"String","StringValue":"1"}}}}`)}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": "two"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a", "payload": "three"}},
	}

	err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected one SQS request, got %d", len(sqs.requests))
	}
	if !reflect.DeepEqual(sqs.requests[0].entries[0].Attributes, map[string]sqsMessageAttribute{"ok": sqsStringAttribute("1")}) {
		t.Fatalf("unexpected sanitized attributes: %#v", sqs.requests[0].entries[0].Attributes)
	}
}

func TestSendSQS10EventsSendsNativeTypedAttributes(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	events := []event{{columns: map[string]any{
		"id":          "event-1",
		"destination": "queue-a",
		"payload":     "one",
		"options": []byte(`{"sqs":{"attributes":{
			"tenant":{"DataType":"String.customer","StringValue":"acme"},
			"attempt":{"DataType":"Number.int","StringValue":"3"},
			"signature":{"DataType":"Binary.sig","BinaryValue":"SGVsbG8="}
		}}}`),
	}}}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
	}

	attributes := sqs.requests[0].entries[0].Attributes
	if attributes["tenant"].DataType != "String.customer" || attributes["tenant"].StringValue != "acme" || !attributes["tenant"].HasStringValue {
		t.Fatalf("unexpected string attribute: %#v", attributes["tenant"])
	}
	if attributes["attempt"].DataType != "Number.int" || attributes["attempt"].StringValue != "3" || !attributes["attempt"].HasStringValue {
		t.Fatalf("unexpected number attribute: %#v", attributes["attempt"])
	}
	if attributes["signature"].DataType != "Binary.sig" || string(attributes["signature"].BinaryValue) != "Hello" || !attributes["signature"].HasBinaryValue {
		t.Fatalf("unexpected binary attribute: %#v", attributes["signature"])
	}
}

func TestSendSQS10EventsRejectsInvalidNativeAttributes(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
	}{
		{name: "shorthand", attribute: `"value"`},
		{name: "missing type", attribute: `{"StringValue":"value"}`},
		{name: "binary base64", attribute: `{"DataType":"Binary","BinaryValue":"%%%"}`},
		{name: "list", attribute: `{"DataType":"String","StringListValues":["a"]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			sqs := &fakeSQSPublisher{autoReply: true}
			a := &app{cfg: cfg, sqs: sqs}
			var deleted []any
			events := []event{{columns: map[string]any{
				"id":          "event-1",
				"destination": "queue-a",
				"payload":     "one",
				"options":     []byte(fmt.Sprintf(`{"sqs":{"attributes":{"bad":%s}}}`, tt.attribute)),
			}}}

			if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(id any) { deleted = append(deleted, id) }); err != nil {
				t.Fatalf("sendSQSBatchForTest returned error: %v", err)
			}
			if !reflect.DeepEqual(deleted, []any{"event-1"}) {
				t.Fatalf("expected invalid attributes to be poison, got %#v", deleted)
			}
			if len(sqs.requests) != 0 {
				t.Fatalf("expected no provider calls for invalid attributes, got %#v", sqs.requests)
			}
		})
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

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
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

	err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(id any) {
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

	err := a.sendSQSBatchForTest(ctx, "queue-a", events, func(id any) {
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
		done <- a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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
	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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

	if err := a.sendSQSEventsForTest(context.Background(), events, func(any) {}); err != nil {
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

	if err := a.sendSQSEventsForTest(context.Background(), events, func(any) {}); err != nil {
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "options": combinedOrderingOptions("group-a")}},
	}

	err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "options": combinedOrderingOptions("group-a")}},
	}

	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": combinedOrderingOptions("group-b")}},
	}

	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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

func TestSendSQSEventsFIFOProcessesWholeSelectedGroup(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "options": combinedOrderingOptions("group-a")}},
	}

	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2", "event-3"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 3 {
		t.Fatalf("expected one request per selected FIFO group event, got %#v", sqs.requests)
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": combinedOrderingOptions("group-b")}},
	}

	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": combinedOrderingOptions("group-a")}},
	}

	if err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "options": map[string]any{"sqs": map[string]any{"messageGroupId": "group-a", "delaySeconds": "invalid"}}}},
	}

	err := a.sendSQSEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if len(deleted) != 0 {
		t.Fatalf("later malformed event must remain after an earlier timeout, got deleted ids %#v", deleted)
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

func TestSendSQS10EventsStandardQueueSendsFairQueueGroup(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "options": combinedOrderingOptions("group-a")}},
	}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.MessageGroupID != "group-a" {
		t.Fatalf("expected fair queue message group id, got %q", entry.MessageGroupID)
	}
	if entry.DeduplicationID != "" {
		t.Fatalf("expected no dedup id on standard queue, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsFIFOUsesExplicitDeduplicationID(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": map[string]any{"sqs": map[string]any{"messageGroupId": "group-a", "messageDeduplicationId": "custom-dedup"}}}},
	}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.DeduplicationID != "custom-dedup" {
		t.Fatalf("expected explicit deduplication id, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsSendsDelayAndTraceHeader(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "options": map[string]any{"sqs": map[string]any{"delaySeconds": 30, "messageSystemAttributes": map[string]any{"AWSTraceHeader": "Root=1-67891233-abcdef012345678912345678"}}}}},
	}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.DelaySeconds == nil || *entry.DelaySeconds != 30 {
		t.Fatalf("expected delay seconds 30, got %#v", entry.DelaySeconds)
	}
	if entry.AWSXRayTraceHeader != "Root=1-67891233-abcdef012345678912345678" {
		t.Fatalf("unexpected trace header: %q", entry.AWSXRayTraceHeader)
	}
}

func TestSendSQS10EventsOmitsDelayForFIFO(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": map[string]any{"sqs": map[string]any{"messageGroupId": "group-a", "delaySeconds": 30}}}},
	}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
	}

	if sqs.requests[0].entries[0].DelaySeconds != nil {
		t.Fatalf("expected FIFO delay seconds to be omitted, got %#v", sqs.requests[0].entries[0].DelaySeconds)
	}
}

func TestSendSQS10EventsDropsInvalidProviderOptionsAsPoison(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]any
	}{
		{name: "dedup", options: map[string]any{"sqs": map[string]any{"messageGroupId": "group-a", "messageDeduplicationId": "bad\ndedup"}}},
		{name: "delay", options: map[string]any{"sqs": map[string]any{"delaySeconds": 901}}},
		{name: "delay type", options: map[string]any{"sqs": map[string]any{"delaySeconds": "30"}}},
		{name: "empty trace", options: map[string]any{"sqs": map[string]any{"messageSystemAttributes": map[string]any{"AWSTraceHeader": ""}}}},
		{name: "unknown system attribute", options: map[string]any{"sqs": map[string]any{"messageSystemAttributes": map[string]any{"Other": "value"}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			sqs := &fakeSQSPublisher{autoReply: true}
			a := &app{cfg: cfg, sqs: sqs}
			var deleted []any

			events := []event{{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "options": tt.options}}}
			if err := a.sendSQSBatchForTest(context.Background(), "queue-a.fifo", events, func(id any) { deleted = append(deleted, id) }); err != nil {
				t.Fatalf("sendSQSBatchForTest returned error: %v", err)
			}
			if !reflect.DeepEqual(deleted, []any{"event-1"}) {
				t.Fatalf("expected invalid option to be deleted as poison, got %#v", deleted)
			}
			if len(sqs.requests) != 0 {
				t.Fatalf("expected no provider calls for invalid options, got %#v", sqs.requests)
			}
		})
	}
}

func TestSendSQS10EventsFIFOWithoutOrderingKeyGetsFallbackGroup(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one"}},
	}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
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
		{columns: map[string]any{"id": rawID, "destination": "queue-a.fifo", "payload": "one", "options": combinedOrderingOptions("group-a")}},
	}

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
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

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
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

	if err := a.sendSQSBatchForTest(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
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

	if err := a.sendSQSBatchForTest(context.Background(), "bad queue url", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSBatchForTest returned error: %v", err)
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

	err := a.sendSQSBatchForTest(context.Background(), "https://sqs.us-east-1.amazonaws.com/123456789012/missing", events, func(id any) {
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

			err := a.sendSQSBatchForTest(context.Background(), "https://sqs.us-east-1.amazonaws.com/123456789012/queue", events, func(id any) {
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
