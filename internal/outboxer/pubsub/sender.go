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

// Target identifies this provider in routing, the event-options section key,
// and failure signatures.
const Target = "pubsub"

// Config contains the relay settings needed by the Pub/Sub provider.
type Config struct {
	PubSubProjectID    string
	PubSubAPIEndpoint  string
	PublishTimeout     time.Duration
	PublishResultGrace time.Duration
}

// Publisher is the Pub/Sub client behavior used by the sender.
type Publisher interface {
	Publish(ctx context.Context, message Message) PublishResult
	Flush(topic string)
	ResumePublish(topic string, orderingKey string)
	Close() error
}

// PublishResult waits for the result of an asynchronous publish.
type PublishResult interface {
	Get(ctx context.Context) (string, error)
}

// Message is a provider-neutral representation of a Pub/Sub message.
type Message struct {
	Topic       string
	Data        []byte
	OrderingKey string
	Attributes  map[string]string
}

type sender struct {
	cfg       Config
	publisher Publisher
}

// NewSender creates a Pub/Sub implementation of provider.Sender.
func NewSender(cfg Config, publisher Publisher) provider.Sender {
	return newSender(cfg, publisher)
}

func newSender(cfg Config, publisher Publisher) *sender {
	return &sender{cfg: cfg, publisher: publisher}
}

func (a *sender) Send(ctx context.Context, events []provider.Event, callbacks provider.Callbacks) error {
	return a.sendPubsubEventsWithCallbacks(ctx, events, callbacks)
}

func (a *sender) sendPubsubEventsWithCallbacks(ctx context.Context, events []provider.Event, callbacks provider.Callbacks) error {
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

		groupID := candidate.evt.Destination + "\x00" + orderingKey
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

func (a *sender) sendPubsubUnorderedEvents(ctx context.Context, events []pubsubCandidateEvent, callbacks provider.Callbacks) error {
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
		messageID, err := a.awaitPubsubResult(ctx, pendingPublish.result, callbacks)
		switch {
		case err == nil:
			a.markPubsubDone(pendingPublish.prepared, messageID, callbacks)
		case errors.Is(err, context.DeadlineExceeded):
			joined = errors.Join(joined, err)
			a.logPubsubFailure(ctx, pendingPublish.prepared, err, callbacks)
		case isPubSubPermanentBackendError(err):
			done, isolateErr := a.sendPubsubIsolated(ctx, pendingPublish.prepared, false, callbacks)
			if !done {
				joined = errors.Join(joined, err)
			}
			joined = errors.Join(joined, isolateErr)
		default:
			joined = errors.Join(joined, err)
			a.logPubsubFailure(ctx, pendingPublish.prepared, err, callbacks)
		}
	}
	return joined
}

func (a *sender) sendPubsubOrderedGroup(ctx context.Context, events []pubsubCandidateEvent, callbacks provider.Callbacks) error {
	for _, candidate := range events {
		prepared, ok := a.preparePubsubEvent(ctx, candidate, callbacks)
		if !ok {
			continue
		}
		prepared, result := a.publishPubsubEvent(ctx, prepared)
		a.publisher.Flush(prepared.message.Topic)

		messageID, err := a.awaitPubsubResult(ctx, result, callbacks)
		switch {
		case err == nil:
			a.markPubsubDone(prepared, messageID, callbacks)
		case errors.Is(err, context.DeadlineExceeded):
			a.logPubsubFailure(ctx, prepared, err, callbacks)
			return errors.Join(provider.ErrFatalAfterCommit, err)
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
			a.logPubsubFailure(ctx, prepared, err, callbacks)
			return err
		}
	}
	return nil
}

func (a *sender) sendPubsubIsolated(ctx context.Context, prepared pubsubPreparedEvent, ordered bool, callbacks provider.Callbacks) (bool, error) {
	prepared, result := a.publishPubsubEvent(ctx, prepared)
	a.publisher.Flush(prepared.message.Topic)
	messageID, err := a.awaitPubsubResult(ctx, result, callbacks)
	if err != nil && ordered && !errors.Is(err, context.DeadlineExceeded) {
		a.publisher.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
	}
	switch {
	case err == nil:
		a.markPubsubDone(prepared, messageID, callbacks)
		return true, nil
	case errors.Is(err, context.DeadlineExceeded) && ordered:
		a.logPubsubFailure(ctx, prepared, err, callbacks)
		return false, errors.Join(provider.ErrFatalAfterCommit, err)
	case isPubSubPermanentBackendError(err):
		callbacks.AddPoisonID(prepared.log.ID, err.Error())
		a.logPubsubFailure(ctx, prepared, err, callbacks)
		return true, nil
	default:
		a.logPubsubFailure(ctx, prepared, err, callbacks)
		return false, err
	}
}

type pubsubPreparedEvent struct {
	log       provider.PublishLog
	message   Message
	startedAt time.Time
}

type pubsubCandidateEvent struct {
	evt         provider.Event
	orderingKey string
}

type pubsubPendingPublish struct {
	prepared pubsubPreparedEvent
	result   PublishResult
}

func (a *sender) parsePubsubCandidate(ctx context.Context, evt provider.Event, callbacks provider.Callbacks) (pubsubCandidateEvent, bool) {
	orderingKey, err := evt.Options.String("orderingKey")
	if err != nil {
		callbacks.RejectMalformedOptions(ctx, Target, evt, "orderingKey", err)
		return pubsubCandidateEvent{}, false
	}
	return pubsubCandidateEvent{evt: evt, orderingKey: orderingKey}, true
}

func (a *sender) preparePubsubEvent(ctx context.Context, candidate pubsubCandidateEvent, callbacks provider.Callbacks) (pubsubPreparedEvent, bool) {
	evt := candidate.evt
	attributes, err := evt.Options.Object("attributes")
	if err != nil {
		callbacks.RejectMalformedOptions(ctx, Target, evt, "attributes", err)
		return pubsubPreparedEvent{}, false
	}

	stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
	if len(deletedAttributes) != 0 {
		slog.Warn("Some attributes were dropped",
			"event_id", evt.ID,
			"event_destination", evt.Destination,
			"dropped_attributes", deletedAttributes,
		)
	}

	log := provider.NewPublishLog(evt, Target)
	log.OrderingKey = candidate.orderingKey
	log.Attributes = stringAttributes

	prepared := pubsubPreparedEvent{
		log: log,
		message: Message{
			Topic:       evt.Destination,
			Data:        evt.Payload,
			OrderingKey: candidate.orderingKey,
			Attributes:  stringAttributes,
		},
	}

	if reason, poison := pubsubPoisonReason(prepared.message); poison {
		callbacks.AddPoisonID(evt.ID, reason)
		slog.Error("Failed to send event",
			"event_id", evt.ID,
			"event_destination", evt.Destination,
			"error", reason,
		)
		return prepared, false
	}

	return prepared, true
}

func (a *sender) publishPubsubEvent(ctx context.Context, prepared pubsubPreparedEvent) (pubsubPreparedEvent, PublishResult) {
	prepared.startedAt = time.Now()
	prepared.log.Sending()
	return prepared, a.publisher.Publish(ctx, prepared.message)
}

func (a *sender) markPubsubDone(prepared pubsubPreparedEvent, messageID string, callbacks provider.Callbacks) {
	callbacks.AddConfirmedID(prepared.log.ID)
	prepared.log.Sent(messageID, time.Since(prepared.startedAt).Seconds())
}

func (a *sender) logPubsubFailure(ctx context.Context, prepared pubsubPreparedEvent, err error, callbacks provider.Callbacks) {
	callbacks.ReportFailure(ctx, "Failed to send event",
		fmt.Sprintf("%s|%s|%s|%s", Target, prepared.message.Topic, prepared.message.OrderingKey, err.Error()),
		"event_id", prepared.log.ID,
		"event_ordering_key", prepared.message.OrderingKey,
		"event_attributes", prepared.message.Attributes,
		"event_target", Target,
		"event_destination", prepared.message.Topic,
		"error", err.Error(),
	)
}

func (a *sender) awaitPubsubResult(ctx context.Context, result PublishResult, callbacks provider.Callbacks) (string, error) {
	defer callbacks.Progress()

	waitCtx, cancel := provider.WithTimeout(ctx, a.cfg.PublishTimeout+a.cfg.PublishResultGrace)
	defer cancel()
	return result.Get(waitCtx)
}
