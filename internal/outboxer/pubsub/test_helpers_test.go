package pubsub

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	gcppubsub "cloud.google.com/go/pubsub/v2"
	"github.com/fvdsn/outboxer/internal/outboxer/provider"
	"google.golang.org/api/googleapi"
)

type appConfig struct {
	Config
	DefaultPubSubTopic string
	SQSEnabled         bool
}

type app struct {
	cfg    appConfig
	pubsub Publisher
}

func testConfig() appConfig {
	return appConfig{
		Config: Config{
			PublishTimeout:     30 * time.Second,
			PublishResultGrace: 5 * time.Second,
		},
		DefaultPubSubTopic: "default",
		SQSEnabled:         true,
	}
}

func (a *app) sendPubsubEventsForTest(ctx context.Context, events []provider.Event, addIDToDelete func(any)) error {
	events = a.routeTestEvents(events)
	return NewSender(a.cfg.Config, a.pubsub).Send(ctx, events, provider.Callbacks{
		AddConfirmedID: addIDToDelete,
		AddPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
}

func (a *app) sendPubsubEventForTest(ctx context.Context, evt provider.Event, addIDToDelete func(any)) error {
	return a.sendPubsubEventsForTest(ctx, []provider.Event{evt}, addIDToDelete)
}

func (a *app) routeTestEvents(events []provider.Event) []provider.Event {
	routed := make([]provider.Event, len(events))
	for i, evt := range events {
		if evt.Destination == "" {
			evt.Destination = a.cfg.DefaultPubSubTopic
		}
		routed[i] = evt
	}
	return routed
}

// fromColumns resolves a raw outbox row into the typed provider.Event the relay
// core hands to senders, mirroring the production providerEvent resolution.
func fromColumns(columns map[string]any) provider.Event {
	return provider.Event{
		ID:          columns["id"],
		Payload:     provider.ValueBytes(columns["payload"]),
		Timestamp:   columns["timestamp"],
		Target:      provider.ValueString(columns["target"]),
		Destination: provider.ValueString(columns["destination"]),
		Options:     columns["options"],
	}
}

func testEvent(id, target, destination, payload, orderingKey string) provider.Event {
	columns := map[string]any{
		"id":      id,
		"payload": payload,
	}
	if target != "" {
		columns["target"] = target
	}
	if destination != "" {
		columns["destination"] = destination
	}
	if orderingKey != "" {
		columns["options"] = combinedOrderingOptions(orderingKey)
	}
	return fromColumns(columns)
}

func combinedOrderingOptions(key string) map[string]any {
	return map[string]any{
		"pubsub": map[string]any{"orderingKey": key},
		"sqs":    map[string]any{"messageGroupId": key},
	}
}

func pubsubOptions(orderingKey string, attributes map[string]any) map[string]any {
	section := map[string]any{}
	if orderingKey != "" {
		section["orderingKey"] = orderingKey
	}
	if attributes != nil {
		section["attributes"] = attributes
	}
	return map[string]any{"pubsub": section}
}

func sortedDeletedIDs(deleted []any) []string {
	ids := make([]string, 0, len(deleted))
	for _, id := range deleted {
		ids = append(ids, fmt.Sprint(id))
	}
	sort.Strings(ids)
	return ids
}

func expectedHundredEventIDs() []string {
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = fmt.Sprintf("event-%03d", i)
	}
	return ids
}

func pubsubMessageCountByTopic(messages []Message) map[string]int {
	counts := map[string]int{}
	for _, message := range messages {
		counts[message.Topic]++
	}
	return counts
}

func pubsubPermanentError(reason string) error {
	return &googleapi.Error{Code: pubsubPermanentBackendErrorCode, Message: fmt.Sprintf("permanent Pub/Sub rejection: %s", reason)}
}

type fakePubSubPublisher struct {
	mu       sync.Mutex
	err      error
	errs     []error
	results  []fakePubSubResult
	messages []Message
	flushes  []string
	resumes  []fakePubSubResume
}

type fakePubSubResume struct {
	topic       string
	orderingKey string
}

type fakePubSubResult struct {
	messageID string
	err       error
	block     bool
	delay     time.Duration
}

func (p *fakePubSubPublisher) Publish(_ context.Context, message Message) PublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	if len(p.results) > 0 {
		result := p.results[0]
		p.results = p.results[1:]
		if result.messageID == "" {
			result.messageID = fmt.Sprintf("published-%d", len(p.messages))
		}
		return result
	}
	err := p.err
	if len(p.errs) > 0 {
		err = p.errs[0]
		p.errs = p.errs[1:]
	}
	return fakePubSubResult{messageID: fmt.Sprintf("published-%d", len(p.messages)), err: err}
}

func (p *fakePubSubPublisher) Flush(topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushes = append(p.flushes, topic)
}

func (p *fakePubSubPublisher) ResumePublish(topic string, orderingKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes = append(p.resumes, fakePubSubResume{topic: topic, orderingKey: orderingKey})
}

func (p *fakePubSubPublisher) Close() error { return nil }

func (r fakePubSubResult) Get(ctx context.Context) (string, error) {
	if r.block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if r.delay > 0 {
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	if r.err != nil {
		return "", r.err
	}
	return r.messageID, nil
}

type concurrentPubSubPublisher struct {
	mu       sync.Mutex
	messages []Message
	started  chan string
	release  chan struct{}
}

type concurrentPubSubResult struct {
	key     string
	started chan string
	release chan struct{}
}

func (p *concurrentPubSubPublisher) Publish(_ context.Context, message Message) PublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	return concurrentPubSubResult{key: message.OrderingKey, started: p.started, release: p.release}
}

func (p *concurrentPubSubPublisher) Flush(string)                 {}
func (p *concurrentPubSubPublisher) ResumePublish(string, string) {}
func (p *concurrentPubSubPublisher) Close() error                 { return nil }
func (r concurrentPubSubResult) Get(context.Context) (string, error) {
	r.started <- r.key
	<-r.release
	return "message-" + r.key, nil
}

type fakeTopicPublisher struct {
	mu        sync.Mutex
	publishes int
	flushes   int
	resumes   []string
	stopCount int
}

func (p *fakeTopicPublisher) Publish(context.Context, *gcppubsub.Message) *gcppubsub.PublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishes++
	return nil
}

func (p *fakeTopicPublisher) Flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushes++
}

func (p *fakeTopicPublisher) ResumePublish(orderingKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes = append(p.resumes, orderingKey)
}

func (p *fakeTopicPublisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopCount++
}

type blockingPubSubPublisher struct{}

func (blockingPubSubPublisher) Publish(_ context.Context, _ Message) PublishResult {
	return fakePubSubResult{block: true}
}
func (blockingPubSubPublisher) Flush(string)                 {}
func (blockingPubSubPublisher) ResumePublish(string, string) {}
func (blockingPubSubPublisher) Close() error                 { return nil }
