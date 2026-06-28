package sqs

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

type appConfig struct {
	Config
	EventTarget        string
	EventDestination   string
	DefaultSQSQueueURL string
	PubSubEnabled      bool
}

type app struct {
	cfg appConfig
	sqs Publisher
}

func testConfig() appConfig {
	return appConfig{
		Config: Config{
			EventID:            "id",
			EventTimestamp:     "timestamp",
			EventPayload:       "payload",
			EventOptions:       "options",
			SQSSendConcurrency: 8,
			PublishTimeout:     30 * time.Second,
		},
		EventTarget:        "target",
		EventDestination:   "destination",
		DefaultSQSQueueURL: "",
		PubSubEnabled:      true,
	}
}

func (a *app) callbacks(addIDToDelete func(any)) Callbacks {
	return Callbacks{
		AddConfirmedID: addIDToDelete,
		AddPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	}
}

func (a *app) sendSQSEventsForTest(ctx context.Context, events []provider.Event, addIDToDelete func(any)) error {
	events = a.routeTestEvents(events)
	return Send(ctx, a.cfg.Config, a.sqs, events, a.callbacks(addIDToDelete))
}

func (a *app) sendSQSBatchForTest(ctx context.Context, queueURL string, events []provider.Event, addIDToDelete func(any)) error {
	events = a.routeTestEvents(events)
	for i := range events {
		events[i].Destination = queueURL
	}
	callbacks := a.callbacks(addIDToDelete)
	s := &sender{cfg: a.cfg.Config, publisher: a.sqs, callbacks: callbacks}
	if !validSQSQueueURL(queueURL) {
		return s.sendSQSQueueEvents(ctx, make(chan struct{}, a.cfg.SQSSendConcurrency), queueURL, events, callbacks)
	}
	prepared := make([]sqsPreparedEvent, 0, len(events))
	for _, evt := range events {
		candidate, ok := s.parseSQSCandidate(ctx, evt, queueURL, callbacks)
		if !ok {
			continue
		}
		item, ok := s.prepareSQSEvent(ctx, candidate, queueURL, strings.HasSuffix(queueURL, ".fifo"), callbacks)
		if ok {
			prepared = append(prepared, item)
		}
	}
	_, err := s.sendSQSBatch(ctx, queueURL, prepared, callbacks)
	return err
}

func (a *app) routeTestEvents(events []provider.Event) []provider.Event {
	routed := make([]provider.Event, len(events))
	for i, evt := range events {
		destination := provider.String(evt, a.cfg.EventDestination)
		if destination == "" {
			destination = a.cfg.DefaultSQSQueueURL
		}
		evt.Destination = destination
		routed[i] = evt
	}
	return routed
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
	return provider.Event{Columns: columns}
}

func combinedOrderingOptions(key string) map[string]any {
	return map[string]any{
		"pubsub": map[string]any{"orderingKey": key},
		"sqs":    map[string]any{"messageGroupId": key},
	}
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

func sqsEntryCountByQueue(requests []fakeSQSRequest) map[string]int {
	counts := map[string]int{}
	for _, request := range requests {
		counts[request.queueURL] += len(request.entries)
	}
	return counts
}

func sqsRequestCountByQueue(requests []fakeSQSRequest) map[string]int {
	counts := map[string]int{}
	for _, request := range requests {
		counts[request.queueURL]++
	}
	return counts
}

type fakeSQSPublisher struct {
	mu        sync.Mutex
	err       error
	errs      []error
	response  BatchResponse
	responses []BatchResponse
	requests  []fakeSQSRequest
	autoReply bool
}

type fakeSQSRequest struct {
	queueURL string
	entries  []BatchEntry
}

type fakeSQSAPIError struct {
	code string
}

func (e fakeSQSAPIError) Error() string     { return e.code }
func (e fakeSQSAPIError) ErrorCode() string { return e.code }

func (p *fakeSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []BatchEntry) (BatchResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]BatchEntry(nil), entries...)})
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]
		if err != nil {
			return BatchResponse{}, err
		}
	}
	if p.err != nil {
		return BatchResponse{}, p.err
	}
	if len(p.responses) > 0 {
		response := p.responses[0]
		p.responses = p.responses[1:]
		return response, nil
	}
	if p.autoReply {
		response := BatchResponse{}
		for _, entry := range entries {
			response.Successful = append(response.Successful, BatchSuccess{
				ID:        entry.ID,
				MessageID: "message-" + entry.ID,
			})
		}
		return response, nil
	}
	return p.response, nil
}

type keyedSQSPublisher struct {
	mu        sync.Mutex
	requests  []fakeSQSRequest
	responses map[string]BatchResponse
}

func (p *keyedSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []BatchEntry) (BatchResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]BatchEntry(nil), entries...)})
	if len(entries) == 0 {
		return BatchResponse{}, nil
	}
	return p.responses[entries[0].ID], nil
}

type trackingSQSPublisher struct {
	mu       sync.Mutex
	inFlight int
	max      int
	started  chan struct{}
	release  chan struct{}
	requests []fakeSQSRequest
}

func (p *trackingSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []BatchEntry) (BatchResponse, error) {
	p.mu.Lock()
	p.inFlight++
	if p.inFlight > p.max {
		p.max = p.inFlight
	}
	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]BatchEntry(nil), entries...)})
	p.mu.Unlock()

	p.started <- struct{}{}
	<-p.release

	p.mu.Lock()
	p.inFlight--
	p.mu.Unlock()

	response := BatchResponse{}
	for _, entry := range entries {
		response.Successful = append(response.Successful, BatchSuccess{
			ID:        entry.ID,
			MessageID: "message-" + entry.ID,
		})
	}
	return response, nil
}

type blockingSQSPublisher struct{}

func (blockingSQSPublisher) SendBatch(ctx context.Context, _ string, _ []BatchEntry) (BatchResponse, error) {
	<-ctx.Done()
	return BatchResponse{}, ctx.Err()
}

type recordingBlockingSQSPublisher struct {
	mu       sync.Mutex
	requests []fakeSQSRequest
}

func (p *recordingBlockingSQSPublisher) SendBatch(ctx context.Context, queueURL string, entries []BatchEntry) (BatchResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]BatchEntry(nil), entries...)})
	p.mu.Unlock()

	<-ctx.Done()
	return BatchResponse{}, ctx.Err()
}
