// Package sqs publishes provider events to Amazon SQS.
package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

// Target identifies this provider in routing, the event-options section key,
// and failure signatures.
const Target = "sqs"

const (
	sqsEventBatchSize   = 10
	sqsEventMaxSizeByte = 1024 * 1024
	sqsMaxAttributes    = 10
	sqsMaxDelaySeconds  = 900

	// WebIdentityProviderGoogle selects Google metadata identity tokens for AWS.
	WebIdentityProviderGoogle = "google"
)

var (
	sqsBatchEntryIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	sqsFIFOIDPattern       = regexp.MustCompile(`^[A-Za-z0-9!"#$%&'()*+,\-./:;<=>?@\[\\\]\^_` + "`" + `{|}~]{1,128}$`)
	sqsAttributeNameRe     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
)

// Config contains the relay settings needed by the SQS provider.
type Config struct {
	SQSSendConcurrency         int
	PublishTimeout             time.Duration
	SQSAPIEndpoint             string
	AWSRegion                  string
	AWSRoleARN                 string
	AWSRoleSessionName         string
	AWSRoleDuration            time.Duration
	AWSCredentialRefreshWindow time.Duration
	AWSWebIdentityProvider     string
	AWSWebIdentityAudience     string
}

// Publisher is the SQS client behavior used by the sender.
type Publisher interface {
	SendBatch(ctx context.Context, queueURL string, entries []BatchEntry) (BatchResponse, error)
}

// BatchEntry is one message in an SQS batch request.
type BatchEntry struct {
	ID                 string
	MessageBody        string
	Attributes         map[string]MessageAttribute
	MessageGroupID     string
	DeduplicationID    string
	DelaySeconds       *int32
	AWSXRayTraceHeader string
}

type sqsCandidateEvent struct {
	evt         provider.Event
	options     provider.Options
	id          any
	orderingKey string
}

func (evt sqsCandidateEvent) fifoGroupID() string {
	if evt.orderingKey != "" {
		return evt.orderingKey
	}
	return syntheticFIFOGroupID(fmt.Sprint(evt.id))
}

type sqsPreparedEvent struct {
	id          any
	timestamp   any
	latency     any
	payloadLen  int
	messageSize int
	orderingKey string
	entry       BatchEntry
}

type sqsQueueEvents struct {
	queue  string
	events []provider.Event
}

// BatchResponse contains the per-entry outcomes of an SQS batch request.
type BatchResponse struct {
	Successful []BatchSuccess
	Failed     []BatchFailure
}

// BatchSuccess identifies an entry accepted by SQS.
type BatchSuccess struct {
	ID        string
	MessageID string
}

// BatchFailure describes an entry rejected by SQS.
type BatchFailure struct {
	ID          string
	Code        string
	Message     string
	SenderFault bool
}

type sender struct {
	cfg       Config
	publisher Publisher
}

// NewSender creates an SQS implementation of provider.Sender.
func NewSender(cfg Config, publisher Publisher) provider.Sender {
	return newSender(cfg, publisher)
}

func newSender(cfg Config, publisher Publisher) *sender {
	return &sender{cfg: cfg, publisher: publisher}
}

func (a *sender) Send(ctx context.Context, events []provider.Event, callbacks provider.Callbacks) error {
	return a.sendSQSEventsWithCallbacks(ctx, events, callbacks)
}

func (a *sender) sendSQSEventsWithCallbacks(ctx context.Context, events []provider.Event, callbacks provider.Callbacks) error {
	eventsByQueue := map[string][]provider.Event{}
	for _, evt := range events {
		queue := evt.Destination
		eventsByQueue[queue] = append(eventsByQueue[queue], evt)
	}

	queueGroups := make([]sqsQueueEvents, 0, len(eventsByQueue))
	for queue, queueEvents := range eventsByQueue {
		queueGroups = append(queueGroups, sqsQueueEvents{queue: queue, events: append([]provider.Event(nil), queueEvents...)})
	}

	sem := make(chan struct{}, a.cfg.SQSSendConcurrency)
	return provider.RunConcurrent(queueGroups, func(group sqsQueueEvents) error {
		return a.sendSQSQueueEvents(ctx, sem, group.queue, group.events, callbacks)
	})
}

func (a *sender) sendSQSQueueEvents(ctx context.Context, sem chan struct{}, queue string, events []provider.Event, callbacks provider.Callbacks) error {
	if !validSQSQueueURL(queue) {
		for _, evt := range events {
			callbacks.AddPoisonID(evt.ID, "SQS queue URL is syntactically invalid")
		}
		logFailure(ctx, callbacks, "Failed to send event batch",
			fmt.Sprintf("%s|%s|invalid-queue-url", Target, queue),
			"event_destination", queue,
			"error", "SQS queue URL is syntactically invalid",
		)
		return nil
	}

	candidates := make([]sqsCandidateEvent, 0, len(events))
	for _, evt := range events {
		candidate, ok := a.parseSQSCandidate(ctx, evt, queue, callbacks)
		if ok {
			candidates = append(candidates, candidate)
		}
	}

	if strings.HasSuffix(queue, ".fifo") {
		return a.sendSQSFIFOEvents(ctx, sem, queue, candidates, callbacks)
	}

	prepared := make([]sqsPreparedEvent, 0, len(candidates))
	for _, candidate := range candidates {
		evt, ok := a.prepareSQSEvent(ctx, candidate, queue, false, callbacks)
		if ok {
			prepared = append(prepared, evt)
		}
	}
	return a.sendSQSStandardEvents(ctx, sem, queue, prepared, callbacks)
}

func (a *sender) sendSQSStandardEvents(ctx context.Context, sem chan struct{}, queue string, queueEvents []sqsPreparedEvent, callbacks provider.Callbacks) error {
	chunks := chunkSQSStandardEvents(queueEvents)
	return provider.RunConcurrent(chunks, func(chunk []sqsPreparedEvent) error {
		batch := append([]sqsPreparedEvent(nil), chunk...)
		_, err := a.sendSQSBatchWithSemaphore(ctx, sem, queue, batch, callbacks)
		return err
	})
}

func chunkSQSStandardEvents(events []sqsPreparedEvent) [][]sqsPreparedEvent {
	chunks := [][]sqsPreparedEvent{}
	current := []sqsPreparedEvent{}
	currentSize := 0

	for _, evt := range events {
		size := evt.messageSize
		if len(current) > 0 && (len(current) >= sqsEventBatchSize || currentSize+size > sqsEventMaxSizeByte) {
			chunks = append(chunks, current)
			current = nil
			currentSize = 0
		}
		current = append(current, evt)
		currentSize += size
	}

	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

func (a *sender) sendSQSFIFOEvents(ctx context.Context, sem chan struct{}, queue string, queueEvents []sqsCandidateEvent, callbacks provider.Callbacks) error {
	groups := map[string][]sqsCandidateEvent{}
	groupOrder := []string{}
	for _, evt := range queueEvents {
		groupID := evt.fifoGroupID()
		if _, ok := groups[groupID]; !ok {
			groupOrder = append(groupOrder, groupID)
		}
		groups[groupID] = append(groups[groupID], evt)
	}

	return provider.RunConcurrent(groupOrder, func(groupID string) error {
		groupEvents := append([]sqsCandidateEvent(nil), groups[groupID]...)
		for _, candidate := range groupEvents {
			prepared, ok := a.prepareSQSEvent(ctx, candidate, queue, true, callbacks)
			if !ok {
				continue
			}
			done, err := a.sendSQSBatchWithSemaphore(ctx, sem, queue, []sqsPreparedEvent{prepared}, callbacks)
			if err != nil {
				return err
			}
			if !done {
				return nil
			}
		}
		return nil
	})
}

func (a *sender) parseSQSCandidate(ctx context.Context, evt provider.Event, queueURL string, callbacks provider.Callbacks) (sqsCandidateEvent, bool) {
	options, err := provider.BackendOptions(evt.Options, Target)
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, queueURL, "", err, callbacks)
		return sqsCandidateEvent{}, false
	}
	orderingKey, err := options.String("messageGroupId")
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, queueURL, "messageGroupId", err, callbacks)
		return sqsCandidateEvent{}, false
	}

	id := evt.ID
	return sqsCandidateEvent{
		evt:         evt,
		options:     options,
		id:          id,
		orderingKey: orderingKey,
	}, true
}

func (a *sender) prepareSQSEvent(ctx context.Context, candidate sqsCandidateEvent, queueURL string, isFIFO bool, callbacks provider.Callbacks) (sqsPreparedEvent, bool) {
	attributes, err := sqsAttributes(candidate.options)
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, queueURL, "attributes", err, callbacks)
		return sqsPreparedEvent{}, false
	}
	deduplicationID, err := candidate.options.String("messageDeduplicationId")
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, queueURL, "messageDeduplicationId", err, callbacks)
		return sqsPreparedEvent{}, false
	}
	delaySeconds, err := sqsDelaySeconds(candidate.options)
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, queueURL, "delaySeconds", err, callbacks)
		return sqsPreparedEvent{}, false
	}
	traceHeader, err := sqsAWSTraceHeader(candidate.options)
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, queueURL, "messageSystemAttributes", err, callbacks)
		return sqsPreparedEvent{}, false
	}

	timestamp := candidate.evt.Timestamp
	eventID := fmt.Sprint(candidate.id)
	entryID := providerSafeID(eventID, sqsBatchEntryIDPattern)
	data := candidate.evt.Payload
	latency := provider.Latency(timestamp)
	if isSQSPoison(data, attributes, candidate.orderingKey, deduplicationID, delaySeconds) {
		callbacks.AddPoisonID(candidate.id, "Event is invalid for SQS")
		logFailure(ctx, callbacks, "Failed to send event",
			fmt.Sprintf("%s|%s|%s|local-poison", Target, queueURL, candidate.orderingKey),
			"event_id", candidate.id,
			"event_destination", queueURL,
			"error", "Event is invalid for SQS",
		)
		return sqsPreparedEvent{}, false
	}

	entry := BatchEntry{
		ID:                 entryID,
		MessageBody:        string(data),
		Attributes:         attributes,
		AWSXRayTraceHeader: traceHeader,
	}
	if isFIFO {
		entry.MessageGroupID = candidate.fifoGroupID()
		if deduplicationID != "" {
			entry.DeduplicationID = deduplicationID
		} else {
			entry.DeduplicationID = providerSafeID(eventID, sqsFIFOIDPattern)
		}
	} else {
		entry.DelaySeconds = delaySeconds
		entry.MessageGroupID = candidate.orderingKey
	}

	return sqsPreparedEvent{
		id:          candidate.id,
		timestamp:   timestamp,
		latency:     latency,
		payloadLen:  len(data),
		messageSize: sqsMessageSize(data, attributes),
		orderingKey: candidate.orderingKey,
		entry:       entry,
	}, true
}

func (a *sender) sendSQSBatchWithSemaphore(ctx context.Context, sem chan struct{}, queue string, events []sqsPreparedEvent, callbacks provider.Callbacks) (bool, error) {
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return a.sendSQSBatch(ctx, queue, events, callbacks)
}

func (a *sender) sendSQSBatch(ctx context.Context, queueURL string, events []sqsPreparedEvent, callbacks provider.Callbacks) (bool, error) {
	if len(events) == 0 {
		return false, nil
	}
	if callbacks.MarkProgress != nil {
		defer callbacks.MarkProgress()
	}

	start := time.Now()
	entries := make([]BatchEntry, 0, len(events))
	idsByEntryID := map[string]any{}

	for _, evt := range events {
		slog.Debug("Sending event",
			"event_id", evt.id,
			"event_timestamp", evt.timestamp,
			"event_latency", evt.latency,
			"event_payload_size", evt.payloadLen,
			"event_ordering_key", evt.orderingKey,
			"event_attributes", evt.entry.Attributes,
			"event_target", Target,
			"event_destination", queueURL,
		)
		entries = append(entries, evt.entry)
		idsByEntryID[evt.entry.ID] = evt.id
	}

	sendCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout)
	defer cancel()
	response, err := a.publisher.SendBatch(sendCtx, queueURL, entries)
	if err != nil {
		if isSQSPermanentRequestError(err) {
			if len(events) == 1 {
				callbacks.AddPoisonID(events[0].id, err.Error())
				logFailure(ctx, callbacks, "Failed to send event",
					fmt.Sprintf("%s|%s|%s", Target, queueURL, err.Error()),
					"event_id", events[0].id,
					"event_destination", queueURL,
					"error", err.Error(),
				)
				return true, nil
			}
			return a.sendSQSBatchIsolated(ctx, queueURL, events, callbacks)
		}
		logFailure(ctx, callbacks, "Failed to send event batch",
			fmt.Sprintf("%s|%s|%s", Target, queueURL, err.Error()),
			"event_destination", queueURL,
			"error", err.Error(),
		)
		return false, err
	}

	publishLatency := time.Since(start).Seconds()
	anyDone := false
	for _, entry := range response.Successful {
		originalID := idsByEntryID[entry.ID]
		callbacks.AddConfirmedID(originalID)
		anyDone = true
		slog.Debug("Event sent",
			"event_id", entry.ID,
			"event_published_id", entry.MessageID,
			"event_destination", queueURL,
			"publish_latency", publishLatency,
		)
	}

	for _, entry := range response.Failed {
		if entry.SenderFault {
			callbacks.AddPoisonID(idsByEntryID[entry.ID], fmt.Sprintf("%s: %s", entry.Code, entry.Message))
			anyDone = true
		}
		logFailure(ctx, callbacks, "Failed to send event",
			fmt.Sprintf("%s|%s|%s|%s", Target, queueURL, entry.Code, entry.Message),
			"event_id", entry.ID,
			"event_destination", queueURL,
			"error", fmt.Sprintf("%s: %s", entry.Code, entry.Message),
		)
	}

	return anyDone, nil
}

func (a *sender) sendSQSBatchIsolated(ctx context.Context, queueURL string, events []sqsPreparedEvent, callbacks provider.Callbacks) (bool, error) {
	anyDone := false
	var joined error
	for _, evt := range events {
		done, err := a.sendSQSBatch(ctx, queueURL, []sqsPreparedEvent{evt}, callbacks)
		if done {
			anyDone = true
		}
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return anyDone, joined
}

func (a *sender) rejectMalformedOptions(ctx context.Context, evt provider.Event, destination string, field string, err error, callbacks provider.Callbacks) {
	signature := fmt.Sprintf("%s|%s|malformed-options", Target, destination)
	if field != "" {
		signature = fmt.Sprintf("%s|%s|%s|malformed-options", Target, destination, field)
	}
	callbacks.AddPoisonID(evt.ID, err.Error())
	logFailure(ctx, callbacks, "Failed to send event",
		signature,
		"event_id", evt.ID,
		"event_destination", destination,
		"error", err.Error(),
	)
}

func logFailure(ctx context.Context, callbacks provider.Callbacks, message string, signature string, attrs ...any) {
	if callbacks.LogFailure != nil {
		callbacks.LogFailure(ctx, message, signature, attrs...)
	}
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
