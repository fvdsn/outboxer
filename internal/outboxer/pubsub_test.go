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

func TestSendPubsubEventRespectsPublishTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 50 * time.Millisecond
	cfg.PublishResultGrace = 0
	a := &app{cfg: cfg, pubsub: blockingPubSubPublisher{}}

	evt := event{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "p"}}

	start := time.Now()
	err := a.sendPubsubEventForTest(context.Background(), evt, func(any) {})
	if err == nil {
		t.Fatal("expected a timeout error from a blocked publish")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("publish blocked for %s instead of timing out", elapsed)
	}
}

func TestSendPubsubEventsUnorderedHundredOneTopic(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}

	events := make([]event, 100)
	for i := range events {
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "pubsub", "topic-1", "payload", "")
	}

	deleted := []any{}
	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if len(pubsub.messages) != 100 {
		t.Fatalf("expected 100 Pub/Sub messages, got %d", len(pubsub.messages))
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1"}) {
		t.Fatalf("expected one flush for topic-1, got %#v", pubsub.flushes)
	}
	if got, want := sortedDeletedIDs(deleted), expectedHundredEventIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deleted IDs:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestSendPubsubEventsUnorderedHundredAcrossTenTopics(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}

	events := make([]event, 100)
	for i := range events {
		topic := fmt.Sprintf("topic-%02d", i/10)
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "pubsub", topic, "payload", "")
	}

	deleted := []any{}
	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if len(pubsub.messages) != 100 {
		t.Fatalf("expected 100 Pub/Sub messages, got %d", len(pubsub.messages))
	}
	sort.Strings(pubsub.flushes)
	expectedFlushes := []string{}
	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("topic-%02d", i)
		expectedFlushes = append(expectedFlushes, topic)
		if got := pubsubMessageCountByTopic(pubsub.messages)[topic]; got != 10 {
			t.Fatalf("expected 10 messages for %s, got %d in %#v", topic, got, pubsub.messages)
		}
	}
	if !reflect.DeepEqual(pubsub.flushes, expectedFlushes) {
		t.Fatalf("unexpected topic flushes:\ngot  %#v\nwant %#v", pubsub.flushes, expectedFlushes)
	}
	if got, want := sortedDeletedIDs(deleted), expectedHundredEventIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deleted IDs:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestSendPubsubEventsUsesDefaultTopicForHundredEvents(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.EventTarget = ""
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = "topic-default"
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}

	events := make([]event, 100)
	for i := range events {
		events[i] = testEvent(fmt.Sprintf("event-%03d", i), "", "", "payload", "")
	}

	deleted := []any{}
	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if got := pubsubMessageCountByTopic(pubsub.messages)["topic-default"]; got != 100 {
		t.Fatalf("expected all messages to use default topic, got %d in %#v", got, pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-default"}) {
		t.Fatalf("expected one flush for default topic, got %#v", pubsub.flushes)
	}
	if got, want := sortedDeletedIDs(deleted), expectedHundredEventIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deleted IDs:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestSendPubsubEventsMixedOrderedAndUnorderedAcrossTopics(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}

	events := []event{}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("unordered-%03d", i)
		events = append(events, testEvent(id, "pubsub", fmt.Sprintf("topic-%d", i%2), id, ""))
	}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("ordered-%03d", i)
		events = append(events, testEvent(id, "pubsub", fmt.Sprintf("topic-%d", i%2), id, fmt.Sprintf("key-%d", i%3)))
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if len(pubsub.messages) != len(events) {
		t.Fatalf("expected %d Pub/Sub messages, got %d", len(events), len(pubsub.messages))
	}
	if got := pubsubMessageCountByTopic(pubsub.messages); !reflect.DeepEqual(got, map[string]int{"topic-0": 16, "topic-1": 16}) {
		t.Fatalf("unexpected message counts by topic: %#v", got)
	}
	for _, message := range pubsub.messages {
		if strings.HasPrefix(string(message.Data), "ordered") && message.OrderingKey == "" {
			t.Fatalf("expected ordered message to keep ordering key: %#v", message)
		}
	}
	deletedMu.Lock()
	deletedCount := len(deleted)
	deletedMu.Unlock()
	if deletedCount != len(events) {
		t.Fatalf("expected all events deleted, got %d of %d: %#v", deletedCount, len(events), deleted)
	}
}

func TestPubSubLocalPrevalidationBoundaries(t *testing.T) {
	attrs := map[string]string{}
	for i := 0; i < pubsubMaxAttributes; i++ {
		attrs[fmt.Sprintf("attr%d", i)] = "value"
	}
	if !validPubSubAttributes(attrs) {
		t.Fatal("expected exactly max Pub/Sub attributes to be valid")
	}
	attrs["overflow"] = "value"
	if validPubSubAttributes(attrs) {
		t.Fatal("expected too many Pub/Sub attributes to be invalid")
	}

	if !validPubSubAttributes(map[string]string{strings.Repeat("k", pubsubMaxAttributeKeyBytes): "value"}) {
		t.Fatal("expected max-length Pub/Sub attribute key to be valid")
	}
	if validPubSubAttributes(map[string]string{strings.Repeat("k", pubsubMaxAttributeKeyBytes+1): "value"}) {
		t.Fatal("expected overlong Pub/Sub attribute key to be invalid")
	}
	if !validPubSubAttributes(map[string]string{"key": strings.Repeat("v", pubsubMaxAttributeValueBytes)}) {
		t.Fatal("expected max-length Pub/Sub attribute value to be valid")
	}
	if validPubSubAttributes(map[string]string{"key": strings.Repeat("v", pubsubMaxAttributeValueBytes+1)}) {
		t.Fatal("expected overlong Pub/Sub attribute value to be invalid")
	}
	if validPubSubAttributes(map[string]string{"googclient": "value"}) {
		t.Fatal("expected goog-prefixed Pub/Sub attribute key to be invalid")
	}

	if reason, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1", Data: make([]byte, pubsubMaxMessageDataBytes)}); poison {
		t.Fatalf("expected exactly max Pub/Sub data to be accepted, got poison: %s", reason)
	}
	if _, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1", Data: make([]byte, pubsubMaxMessageDataBytes+1)}); !poison {
		t.Fatal("expected overlarge Pub/Sub data to be poison")
	}
	if _, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1", OrderingKey: "key-a"}); poison {
		t.Fatal("expected ordering-key-only Pub/Sub message not to be local poison")
	}
	if _, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1"}); !poison {
		t.Fatal("expected empty Pub/Sub message with no attributes or key to be poison")
	}
}

func TestPubSubTopicSyntaxValidation(t *testing.T) {
	valid := []string{
		"abc",
		"topic-1",
		"projects/project-a/topics/topic-1",
		"projects/123/topics/a.b_c~d+e%f",
	}
	for _, topic := range valid {
		if !validPubSubTopic(topic) {
			t.Fatalf("expected Pub/Sub topic %q to be valid", topic)
		}
	}

	invalid := []string{
		"",
		"ab",
		"1topic",
		"googtopic",
		"bad/topic",
		"projects/project-a/topics/1bad",
		"projects//topics/topic-1",
		"projects/project-a/subscriptions/sub-1",
	}
	for _, topic := range invalid {
		if validPubSubTopic(topic) {
			t.Fatalf("expected Pub/Sub topic %q to be invalid", topic)
		}
	}
}

func TestSendPubsubEventUsesDefaultTopicAndSanitizesAttributes(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	evt := event{columns: map[string]any{
		"id":      "event-1",
		"payload": "payload",
		"options": []byte(`{"pubsub":{"attributes":{"keep":"yes","drop":42}}}`),
	}}

	err := a.sendPubsubEventForTest(context.Background(), evt, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendPubsubEvent returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(pubsub.messages))
	}

	message := pubsub.messages[0]
	if message.Topic != "default" {
		t.Fatalf("expected default topic, got %q", message.Topic)
	}
	if string(message.Data) != "payload" {
		t.Fatalf("expected payload body, got %q", string(message.Data))
	}
	if !reflect.DeepEqual(message.Attributes, map[string]string{"keep": "yes"}) {
		t.Fatalf("unexpected attributes: %#v", message.Attributes)
	}
}

func TestCloudPubSubPublisherReusesCachedPublisherPerTopic(t *testing.T) {
	created := map[string]int{}
	publishers := map[string]*fakeTopicPublisher{}
	publisher := &cloudPubSubPublisher{
		publishers: map[string]pubsubTopicPublisher{},
	}
	publisher.newPublisher = func(topic string) pubsubTopicPublisher {
		created[topic]++
		topicPublisher := &fakeTopicPublisher{}
		publishers[topic] = topicPublisher
		return topicPublisher
	}

	publisher.Flush("topic-1")
	publisher.ResumePublish("topic-1", "key-a")
	publisher.Flush("topic-1")
	publisher.Flush("topic-2")

	if !reflect.DeepEqual(created, map[string]int{"topic-1": 1, "topic-2": 1}) {
		t.Fatalf("unexpected publisher creation counts: %#v", created)
	}
	if publishers["topic-1"].flushes != 2 {
		t.Fatalf("expected cached topic-1 publisher to be flushed twice, got %d", publishers["topic-1"].flushes)
	}
	if !reflect.DeepEqual(publishers["topic-1"].resumes, []string{"key-a"}) {
		t.Fatalf("unexpected topic-1 resumes: %#v", publishers["topic-1"].resumes)
	}
}

func TestCloudPubSubPublisherCloseStopsCachedPublishers(t *testing.T) {
	publishers := map[string]*fakeTopicPublisher{}
	publisher := &cloudPubSubPublisher{
		publishers: map[string]pubsubTopicPublisher{},
	}
	publisher.newPublisher = func(topic string) pubsubTopicPublisher {
		topicPublisher := &fakeTopicPublisher{}
		publishers[topic] = topicPublisher
		return topicPublisher
	}

	publisher.Flush("topic-1")
	publisher.Flush("topic-2")
	if err := publisher.Close(); err != nil {
		t.Fatalf("close publisher: %v", err)
	}

	for topic, topicPublisher := range publishers {
		if topicPublisher.stopCount != 1 {
			t.Fatalf("expected topic %s to be stopped once, got %d", topic, topicPublisher.stopCount)
		}
	}
}

func TestSendPubsubEventReturnsPublisherError(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("pubsub unavailable")
	a := &app{cfg: cfg, pubsub: &fakePubSubPublisher{err: expectedErr}}

	evt := event{columns: map[string]any{
		"id":          "event-1",
		"destination": "topic-1",
		"payload":     "payload",
	}}

	err := a.sendPubsubEventForTest(context.Background(), evt, func(any) {})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected publisher error, got %v", err)
	}
}

func TestSendPubsubEventKeepsSyntacticallyValidMissingTopic(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("topic not found")
	a := &app{cfg: cfg, pubsub: &fakePubSubPublisher{err: expectedErr}}
	var deleted []any

	evt := event{columns: map[string]any{
		"id":          "event-1",
		"destination": "topic-1",
		"payload":     "payload",
	}}

	err := a.sendPubsubEventForTest(context.Background(), evt, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected publisher error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("expected missing topic event to be kept, got deleted ids %#v", deleted)
	}
}

func TestSendPubsubEventsFlushesUnorderedBatch(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two"}},
	}

	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected two published messages, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1"}) {
		t.Fatalf("expected one flush for topic-1, got %#v", pubsub.flushes)
	}
}

func TestSendPubsubEventsFlushesEachUnorderedTopic(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-2", "payload": "two"}},
	}

	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	sort.Strings(pubsub.flushes)
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1", "topic-2"}) {
		t.Fatalf("expected flush per unordered topic, got %#v", pubsub.flushes)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsOrderedKeySuccessIsSequential(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "options": combinedOrderingOptions("key-a")}},
	}

	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2", "event-3"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 3 {
		t.Fatalf("expected all selected ordered messages to publish, got %#v", pubsub.messages)
	}
	for i, message := range pubsub.messages {
		if got, want := string(message.Data), []string{"one", "two", "three"}[i]; got != want {
			t.Fatalf("message %d data = %q, want %q", i, got, want)
		}
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1", "topic-1", "topic-1"}) {
		t.Fatalf("expected per-message ordered flushes, got %#v", pubsub.flushes)
	}
}

func TestSendPubsubEventsOrderedKeyPreservesOrderAcrossBatches(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}

	firstBatch := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "options": combinedOrderingOptions("key-a")}},
	}
	if err := a.sendPubsubEventsForTest(context.Background(), firstBatch, func(any) {}); err != nil {
		t.Fatalf("first sendPubsubEvents returned error: %v", err)
	}

	secondBatch := []event{
		{columns: map[string]any{"id": "event-4", "destination": "topic-1", "payload": "four", "options": combinedOrderingOptions("key-a")}},
	}
	if err := a.sendPubsubEventsForTest(context.Background(), secondBatch, func(any) {}); err != nil {
		t.Fatalf("second sendPubsubEvents returned error: %v", err)
	}

	got := []string{}
	for _, message := range pubsub.messages {
		got = append(got, string(message.Data))
	}
	if !reflect.DeepEqual(got, []string{"one", "two", "three", "four"}) {
		t.Fatalf("unexpected ordered publish sequence: %#v", got)
	}
}

func TestSendPubsubEventsOrderedKeysProgressConcurrently(t *testing.T) {
	cfg := testConfig()
	pubsub := &concurrentPubSubPublisher{
		started: make(chan string, 2),
		release: make(chan struct{}, 2),
	}
	a := &app{cfg: cfg, pubsub: pubsub}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": combinedOrderingOptions("key-b")}},
	}

	done := make(chan error, 1)
	go func() {
		done <- a.sendPubsubEventsForTest(context.Background(), events, func(any) {})
	}()

	started := []string{}
	for i := 0; i < 2; i++ {
		select {
		case key := <-pubsub.started:
			started = append(started, key)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ordered keys to start")
		}
	}
	sort.Strings(started)
	if !reflect.DeepEqual(started, []string{"key-a", "key-b"}) {
		t.Fatalf("expected both ordered keys to wait concurrently, got %#v", started)
	}

	pubsub.release <- struct{}{}
	pubsub.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}
}

func TestSendPubsubEventsMixedOrderedAndUnorderedSuccess(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "ordered-1", "destination": "topic-1", "payload": "ordered", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "unordered-1", "destination": "topic-1", "payload": "unordered"}},
	}

	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}
	sort.Slice(deleted, func(i, j int) bool { return fmt.Sprint(deleted[i]) < fmt.Sprint(deleted[j]) })
	if !reflect.DeepEqual(deleted, []any{"ordered-1", "unordered-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected two Pub/Sub messages, got %#v", pubsub.messages)
	}
}

func TestSendPubsubEventsUnorderedUnknownResultIsKept(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	cfg.PublishResultGrace = 0
	pubsub := &fakePubSubPublisher{results: []fakePubSubResult{{block: true}, {}}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two"}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsWaitsThroughPublishResultGrace(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	cfg.PublishResultGrace = 80 * time.Millisecond
	pubsub := &fakePubSubPublisher{results: []fakePubSubResult{{delay: 50 * time.Millisecond}}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
	}

	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsCanceledResultIsKept(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = time.Hour
	pubsub := &fakePubSubPublisher{results: []fakePubSubResult{{block: true}}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
	}

	err := a.sendPubsubEventsForTest(ctx, events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsDoesNotPoisonMultiEventPublishLimits(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	largeEvents := []event{
		{columns: map[string]any{"id": "large-1", "destination": "topic-1", "payload": strings.Repeat("a", 6_000_000)}},
		{columns: map[string]any{"id": "large-2", "destination": "topic-1", "payload": strings.Repeat("b", 6_000_000)}},
	}
	if err := a.sendPubsubEventsForTest(context.Background(), largeEvents, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents for large events returned error: %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"large-1", "large-2"}) {
		t.Fatalf("unexpected deleted ids for large events: %#v", deleted)
	}

	manyEvents := make([]event, pubsubMaxPublishRequestMessages+1)
	for i := range manyEvents {
		manyEvents[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("many-%04d", i),
			"destination": "topic-1",
			"payload":     "payload",
		}}
	}
	deleted = nil
	if err := a.sendPubsubEventsForTest(context.Background(), manyEvents, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents for many events returned error: %v", err)
	}
	if len(deleted) != len(manyEvents) {
		t.Fatalf("expected all many events deleted, got %d of %d", len(deleted), len(manyEvents))
	}
}

func TestSendPubsubEventsDropsLocalPoisonWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	tooManyAttributes := map[string]any{}
	for i := 0; i < pubsubMaxAttributes+1; i++ {
		tooManyAttributes[fmt.Sprintf("attr%d", i)] = "value"
	}
	events := []event{
		{columns: map[string]any{"id": "empty", "destination": "topic-1", "payload": ""}},
		{columns: map[string]any{"id": "large", "destination": "topic-1", "payload": strings.Repeat("x", pubsubMaxMessageDataBytes+1)}},
		{columns: map[string]any{"id": "attrs", "destination": "topic-1", "payload": "body", "options": pubsubOptions("", tooManyAttributes)}},
		{columns: map[string]any{"id": "topic", "destination": "1-bad-topic", "payload": "body"}},
	}

	if err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"empty", "large", "attrs", "topic"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no provider calls for local poison, got %#v", pubsub.messages)
	}
}

func TestSendPubsubEventsIsolatesPermanentUnorderedFailure(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{errs: []error{pubsubPermanentError("bundle"), nil}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "payload"}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected initial publish plus isolated retry, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1", "topic-1"}) {
		t.Fatalf("expected flush for initial publish and isolation, got %#v", pubsub.flushes)
	}
}

func TestSendPubsubEventsIsolatesPermanentBadEventAndValidEvent(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{errs: []error{
		pubsubPermanentError("bundle"),
		pubsubPermanentError("bundle"),
		pubsubPermanentError("bad event"),
		nil,
	}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "bad", "destination": "topic-1", "payload": "bad"}},
		{columns: map[string]any{"id": "valid", "destination": "topic-1", "payload": "valid"}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"bad", "valid"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 4 {
		t.Fatalf("expected two bundled publishes plus two isolated publishes, got %#v", pubsub.messages)
	}
}

func TestSendPubsubEventsOrderedRetryableFailureResumesAndStopsKey(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("retryable")
	pubsub := &fakePubSubPublisher{err: expectedErr}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": map[string]any{"pubsub": map[string]any{"orderingKey": "key-a", "attributes": 42}}}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected retryable error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("later malformed event must remain after an earlier retryable failure, got deleted ids %#v", deleted)
	}
	if len(pubsub.messages) != 1 {
		t.Fatalf("expected only first key event to be published, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.resumes, []fakePubSubResume{{topic: "topic-1", orderingKey: "key-a"}}) {
		t.Fatalf("unexpected resumes: %#v", pubsub.resumes)
	}
}

func TestSendPubsubEventsOrderedFailureAfterSuccessKeepsRemainder(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("retryable")
	pubsub := &fakePubSubPublisher{errs: []error{nil, expectedErr}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "options": combinedOrderingOptions("key-a")}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected retryable error, got %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected only first two key events to be published, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.resumes, []fakePubSubResume{{topic: "topic-1", orderingKey: "key-a"}}) {
		t.Fatalf("unexpected resumes: %#v", pubsub.resumes)
	}
}

func TestSendPubsubEventsOrderedIsolationStopsAtFirstNonDone(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("still retryable")
	pubsub := &fakePubSubPublisher{errs: []error{pubsubPermanentError("bundle"), expectedErr}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": combinedOrderingOptions("key-a")}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected isolated retryable error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected initial publish plus isolated retry, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.resumes, []fakePubSubResume{
		{topic: "topic-1", orderingKey: "key-a"},
		{topic: "topic-1", orderingKey: "key-a"},
	}) {
		t.Fatalf("unexpected resumes: %#v", pubsub.resumes)
	}
}

func TestSendPubsubEventsOrderedUnknownResultIsFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	cfg.PublishResultGrace = 0
	a := &app{cfg: cfg, pubsub: blockingPubSubPublisher{}}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "options": combinedOrderingOptions("key-a")}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "options": combinedOrderingOptions("key-a")}},
	}

	err := a.sendPubsubEventsForTest(context.Background(), events, func(any) {})
	if !errors.Is(err, errFatalAfterCommit) {
		t.Fatalf("expected fatal-after-commit error, got %v", err)
	}
}
