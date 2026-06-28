// Package pubsub publishes provider events to Google Cloud Pub/Sub.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

const targetPubSub = "pubsub"

var ErrFatalAfterCommit = errors.New("fatal after commit")

type Config struct {
	EventID            string
	EventTimestamp     string
	EventPayload       string
	EventTarget        string
	EventOptions       string
	PubSubProjectID    string
	PubSubAPIEndpoint  string
	PublishTimeout     time.Duration
	PublishResultGrace time.Duration
}

type Callbacks struct {
	AddConfirmedID func(any)
	AddPoisonID    func(any, string)
	MarkProgress   func()
	LogFailure     func(context.Context, string, string, ...any)
}

type Publisher interface {
	Publish(ctx context.Context, message Message) PublishResult
	Flush(topic string)
	ResumePublish(topic string, orderingKey string)
	Close() error
}

type PublishResult interface {
	Get(ctx context.Context) (string, error)
}

type Message struct {
	Topic       string
	Data        []byte
	OrderingKey string
	Attributes  map[string]string
}

type sender struct {
	cfg       Config
	publisher Publisher
	callbacks Callbacks
}

func Send(ctx context.Context, cfg Config, publisher Publisher, events []provider.Event, callbacks Callbacks) error {
	a := &sender{cfg: cfg, publisher: publisher, callbacks: callbacks}
	return a.sendPubsubEventsWithCallbacks(ctx, events, callbacks)
}

func (a *sender) sendPubsubEventsWithCallbacks(ctx context.Context, events []provider.Event, callbacks Callbacks) error {
	unordered := []pubsubCandidateEvent{}
	orderedByGroup := map[string][]pubsubCandidateEvent{}
	groupOrder := []string{}

	for _, evt := range events {
		candidate, ok := a.parsePubsubCandidate(ctx, evt, callbacks)
		if !ok {
			continue
		}
		orderingKey := candidate.orderingKey
		if orderingKey == "" {
			unordered = append(unordered, candidate)
			continue
		}

		topic := candidate.topic
		groupID := topic + "\x00" + orderingKey
		if _, ok := orderedByGroup[groupID]; !ok {
			groupOrder = append(groupOrder, groupID)
		}
		orderedByGroup[groupID] = append(orderedByGroup[groupID], candidate)
	}

	unorderedErr := a.sendPubsubUnorderedEvents(ctx, unordered, callbacks)
	orderedErr := provider.RunConcurrent(groupOrder, func(groupID string) error {
		groupEvents := append([]pubsubCandidateEvent(nil), orderedByGroup[groupID]...)
		return a.sendPubsubOrderedGroup(ctx, groupEvents, callbacks)
	})
	return errors.Join(unorderedErr, orderedErr)
}

func (a *sender) sendPubsubUnorderedEvents(ctx context.Context, events []pubsubCandidateEvent, callbacks Callbacks) error {
	pending := []pubsubPendingPublish{}
	topics := map[string]struct{}{}
	for _, candidate := range events {
		prepared, ok := a.preparePubsubEvent(ctx, candidate, callbacks)
		if !ok {
			continue
		}
		prepared, result := a.publishPubsubEvent(ctx, prepared)
		pending = append(pending, pubsubPendingPublish{prepared: prepared, result: result})
		topics[prepared.message.Topic] = struct{}{}
	}

	for topic := range topics {
		a.publisher.Flush(topic)
	}

	var joined error
	for _, pendingPublish := range pending {
		messageID, err := a.awaitPubsubResult(ctx, pendingPublish.result)
		switch {
		case err == nil:
			a.markPubsubDone(pendingPublish.prepared, messageID, callbacks)
		case errors.Is(err, context.DeadlineExceeded):
			joined = errors.Join(joined, err)
			a.logPubsubFailure(ctx, pendingPublish.prepared, err)
		case isPubSubPermanentBackendError(err):
			done, isolateErr := a.sendPubsubIsolated(ctx, pendingPublish.prepared, false, callbacks)
			if !done {
				joined = errors.Join(joined, err)
			}
			joined = errors.Join(joined, isolateErr)
		default:
			joined = errors.Join(joined, err)
			a.logPubsubFailure(ctx, pendingPublish.prepared, err)
		}
	}
	return joined
}

func (a *sender) sendPubsubOrderedGroup(ctx context.Context, events []pubsubCandidateEvent, callbacks Callbacks) error {
	for _, candidate := range events {
		prepared, ok := a.preparePubsubEvent(ctx, candidate, callbacks)
		if !ok {
			continue
		}
		prepared, result := a.publishPubsubEvent(ctx, prepared)
		a.publisher.Flush(prepared.message.Topic)

		messageID, err := a.awaitPubsubResult(ctx, result)
		switch {
		case err == nil:
			a.markPubsubDone(prepared, messageID, callbacks)
		case errors.Is(err, context.DeadlineExceeded):
			a.logPubsubFailure(ctx, prepared, err)
			return errors.Join(ErrFatalAfterCommit, err)
		case isPubSubPermanentBackendError(err):
			a.publisher.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
			done, isolateErr := a.sendPubsubIsolated(ctx, prepared, true, callbacks)
			if isolateErr != nil {
				return isolateErr
			}
			if !done {
				return err
			}
		default:
			a.publisher.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
			a.logPubsubFailure(ctx, prepared, err)
			return err
		}
	}
	return nil
}

func (a *sender) sendPubsubIsolated(ctx context.Context, prepared pubsubPreparedEvent, ordered bool, callbacks Callbacks) (bool, error) {
	prepared, result := a.publishPubsubEvent(ctx, prepared)
	a.publisher.Flush(prepared.message.Topic)
	messageID, err := a.awaitPubsubResult(ctx, result)
	if err != nil && ordered && !errors.Is(err, context.DeadlineExceeded) {
		a.publisher.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
	}
	switch {
	case err == nil:
		a.markPubsubDone(prepared, messageID, callbacks)
		return true, nil
	case errors.Is(err, context.DeadlineExceeded) && ordered:
		a.logPubsubFailure(ctx, prepared, err)
		return false, errors.Join(ErrFatalAfterCommit, err)
	case isPubSubPermanentBackendError(err):
		callbacks.AddPoisonID(prepared.id, err.Error())
		a.logPubsubFailure(ctx, prepared, err)
		return true, nil
	default:
		a.logPubsubFailure(ctx, prepared, err)
		return false, err
	}
}

type pubsubPreparedEvent struct {
	id         any
	timestamp  any
	latency    any
	target     string
	message    Message
	startedAt  time.Time
	payloadLen int
}

type pubsubCandidateEvent struct {
	evt         provider.Event
	options     provider.Options
	topic       string
	orderingKey string
}

type pubsubPendingPublish struct {
	prepared pubsubPreparedEvent
	result   PublishResult
}

func (a *sender) parsePubsubCandidate(ctx context.Context, evt provider.Event, callbacks Callbacks) (pubsubCandidateEvent, bool) {
	topicName := evt.Destination
	options, err := provider.BackendOptions(evt, a.cfg.EventOptions, targetPubSub)
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, topicName, "", err, callbacks)
		return pubsubCandidateEvent{}, false
	}
	orderingKey, err := options.String("orderingKey")
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, topicName, "orderingKey", err, callbacks)
		return pubsubCandidateEvent{}, false
	}
	return pubsubCandidateEvent{evt: evt, options: options, topic: topicName, orderingKey: orderingKey}, true
}

func (a *sender) preparePubsubEvent(ctx context.Context, candidate pubsubCandidateEvent, callbacks Callbacks) (pubsubPreparedEvent, bool) {
	evt := candidate.evt
	attributes, err := candidate.options.Object("attributes")
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, candidate.topic, "attributes", err, callbacks)
		return pubsubPreparedEvent{}, false
	}
	timestamp := provider.Value(evt, a.cfg.EventTimestamp)
	id := provider.Value(evt, a.cfg.EventID)
	data := provider.Bytes(evt, a.cfg.EventPayload)
	latency := provider.Latency(timestamp)
	target := provider.String(evt, a.cfg.EventTarget)

	stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
	if len(deletedAttributes) != 0 {
		slog.Warn("Some attributes were dropped",
			"event_id", id,
			"event_destination", candidate.topic,
			"dropped_attributes", deletedAttributes,
		)
	}

	prepared := pubsubPreparedEvent{
		id:         id,
		timestamp:  timestamp,
		latency:    latency,
		target:     target,
		payloadLen: len(data),
		message: Message{
			Topic:       candidate.topic,
			Data:        data,
			OrderingKey: candidate.orderingKey,
			Attributes:  stringAttributes,
		},
	}

	if reason, poison := pubsubPoisonReason(prepared.message); poison {
		callbacks.AddPoisonID(id, reason)
		slog.Error("Failed to send event",
			"event_id", id,
			"event_destination", candidate.topic,
			"error", reason,
		)
		return prepared, false
	}

	return prepared, true
}

func (a *sender) publishPubsubEvent(ctx context.Context, prepared pubsubPreparedEvent) (pubsubPreparedEvent, PublishResult) {
	prepared.startedAt = time.Now()
	slog.Debug("Sending event",
		"event_id", prepared.id,
		"event_timestamp", prepared.timestamp,
		"event_latency", prepared.latency,
		"event_payload_size", prepared.payloadLen,
		"event_ordering_key", prepared.message.OrderingKey,
		"event_attributes", prepared.message.Attributes,
		"event_target", prepared.target,
		"event_destination", prepared.message.Topic,
	)
	return prepared, a.publisher.Publish(ctx, prepared.message)
}

func (a *sender) markPubsubDone(prepared pubsubPreparedEvent, messageID string, callbacks Callbacks) {
	callbacks.AddConfirmedID(prepared.id)
	slog.Debug("Event sent",
		"event_id", prepared.id,
		"event_timestamp", prepared.timestamp,
		"event_latency", prepared.latency,
		"event_payload_size", prepared.payloadLen,
		"event_published_id", messageID,
		"event_ordering_key", prepared.message.OrderingKey,
		"event_attributes", prepared.message.Attributes,
		"event_target", prepared.target,
		"event_destination", prepared.message.Topic,
		"publish_latency", time.Since(prepared.startedAt).Seconds(),
	)
}

func (a *sender) logPubsubFailure(ctx context.Context, prepared pubsubPreparedEvent, err error) {
	a.logFailure(ctx, "Failed to send event",
		fmt.Sprintf("pubsub|%s|%s|%s", prepared.message.Topic, prepared.message.OrderingKey, err.Error()),
		"event_id", prepared.id,
		"event_ordering_key", prepared.message.OrderingKey,
		"event_attributes", prepared.message.Attributes,
		"event_target", prepared.target,
		"event_destination", prepared.message.Topic,
		"error", err.Error(),
	)
}

func (a *sender) rejectMalformedOptions(ctx context.Context, evt provider.Event, destination string, field string, err error, callbacks Callbacks) {
	signature := fmt.Sprintf("%s|%s|malformed-options", targetPubSub, destination)
	if field != "" {
		signature = fmt.Sprintf("%s|%s|%s|malformed-options", targetPubSub, destination, field)
	}
	callbacks.AddPoisonID(provider.Value(evt, a.cfg.EventID), err.Error())
	a.logFailure(ctx, "Failed to send event",
		signature,
		"event_id", provider.Value(evt, a.cfg.EventID),
		"event_destination", destination,
		"error", err.Error(),
	)
}

func (a *sender) logFailure(ctx context.Context, message string, signature string, attrs ...any) {
	if a.callbacks.LogFailure != nil {
		a.callbacks.LogFailure(ctx, message, signature, attrs...)
	}
}

func (a *sender) markProgress() {
	if a.callbacks.MarkProgress != nil {
		a.callbacks.MarkProgress()
	}
}

func (a *sender) awaitPubsubResult(ctx context.Context, result PublishResult) (string, error) {
	defer a.markProgress()

	waitCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout+a.cfg.PublishResultGrace)
	defer cancel()
	return result.Get(waitCtx)
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
