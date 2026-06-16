package outboxer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	pubsubMaxMessageDataBytes       = 10_000_000
	pubsubMaxPublishRequestMessages = 1000
	pubsubMaxAttributes             = 100
	pubsubMaxAttributeKeyBytes      = 256
	pubsubMaxAttributeValueBytes    = 1024
	pubsubPermanentBackendErrorCode = 400
)

var pubsubTopicIDPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$`)

type pubsubPublisher interface {
	Publish(ctx context.Context, message pubsubMessage) pubsubPublishResult
	Flush(topic string)
	ResumePublish(topic string, orderingKey string)
	Close() error
}

type pubsubPublishResult interface {
	Get(ctx context.Context) (string, error)
}

type pubsubMessage struct {
	Topic       string
	Data        []byte
	OrderingKey string
	Attributes  map[string]string
}

type cloudPubSubPublisher struct {
	client *pubsub.Client
	cfg    appConfig

	mu           sync.Mutex
	publishers   map[string]pubsubTopicPublisher
	newPublisher func(topic string) pubsubTopicPublisher
}

type cloudPubSubPublishResult struct {
	result *pubsub.PublishResult
}

type pubsubTopicPublisher interface {
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
	Flush()
	ResumePublish(orderingKey string)
	Stop()
}

func newPubSubClient(ctx context.Context, cfg appConfig) (*pubsub.Client, error) {
	options := []option.ClientOption{}
	if cfg.PubSubAPIEndpoint != "" {
		options = append(options, option.WithEndpoint(cfg.PubSubAPIEndpoint))
	}
	return pubsub.NewClient(ctx, cfg.PubSubProjectID, options...)
}

func newCloudPubSubPublisher(client *pubsub.Client, cfg appConfig) *cloudPubSubPublisher {
	p := &cloudPubSubPublisher{
		client:     client,
		cfg:        cfg,
		publishers: map[string]pubsubTopicPublisher{},
	}
	p.newPublisher = func(topic string) pubsubTopicPublisher {
		publisher := client.Publisher(topic)
		publisher.EnableMessageOrdering = true
		publisher.PublishSettings.Timeout = cfg.PublishTimeout
		return publisher
	}
	return p
}

func (p *cloudPubSubPublisher) Publish(ctx context.Context, message pubsubMessage) pubsubPublishResult {
	pubsubMsg := &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	}
	if message.OrderingKey != "" {
		pubsubMsg.OrderingKey = message.OrderingKey
	}
	return cloudPubSubPublishResult{result: p.publisher(message.Topic).Publish(ctx, pubsubMsg)}
}

func (p *cloudPubSubPublisher) Flush(topic string) {
	p.publisher(topic).Flush()
}

func (p *cloudPubSubPublisher) ResumePublish(topic string, orderingKey string) {
	if orderingKey == "" {
		return
	}
	p.publisher(topic).ResumePublish(orderingKey)
}

func (p *cloudPubSubPublisher) Close() error {
	p.mu.Lock()
	publishers := make([]pubsubTopicPublisher, 0, len(p.publishers))
	for _, publisher := range p.publishers {
		publishers = append(publishers, publisher)
	}
	p.mu.Unlock()

	for _, publisher := range publishers {
		publisher.Stop()
	}
	return nil
}

func (p *cloudPubSubPublisher) publisher(topic string) pubsubTopicPublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	publisher, ok := p.publishers[topic]
	if ok {
		return publisher
	}

	publisher = p.newPublisher(topic)
	p.publishers[topic] = publisher
	return publisher
}

func (r cloudPubSubPublishResult) Get(ctx context.Context) (string, error) {
	return r.result.Get(ctx)
}

func (a *app) sendPubsubEvents(ctx context.Context, tx *sql.Tx, events []event, addIDToDelete func(any)) error {
	unordered := []event{}
	orderedByGroup := map[string][]event{}
	groupOrder := []string{}

	for _, evt := range events {
		orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
		if orderingKey == "" {
			unordered = append(unordered, evt)
			continue
		}

		topic := a.destinationForBackend(evt, backendPubSub)
		groupID := topic + "\x00" + orderingKey
		if _, ok := orderedByGroup[groupID]; !ok {
			groupOrder = append(groupOrder, groupID)
		}
		if len(orderedByGroup[groupID]) < a.cfg.OrderedGroupBatchCap {
			orderedByGroup[groupID] = append(orderedByGroup[groupID], evt)
		}
	}

	var joined error
	if err := a.sendPubsubUnorderedEvents(ctx, unordered, addIDToDelete); err != nil {
		joined = errors.Join(joined, err)
	}

	errs := make(chan error, len(groupOrder))
	var wg sync.WaitGroup
	for _, groupID := range groupOrder {
		groupEvents := append([]event(nil), orderedByGroup[groupID]...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.sendPubsubOrderedGroup(ctx, groupEvents, addIDToDelete); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (a *app) sendPubsubUnorderedEvents(ctx context.Context, events []event, addIDToDelete func(any)) error {
	pending := []pubsubPendingPublish{}
	topics := map[string]struct{}{}
	for _, evt := range events {
		prepared, ok := a.preparePubsubEvent(evt, false, addIDToDelete)
		if !ok {
			continue
		}
		result := a.pubsub.Publish(ctx, prepared.message)
		pending = append(pending, pubsubPendingPublish{prepared: prepared, result: result})
		topics[prepared.message.Topic] = struct{}{}
	}

	for topic := range topics {
		a.pubsub.Flush(topic)
	}

	var joined error
	for _, pendingPublish := range pending {
		messageID, err := a.awaitPubsubResult(ctx, pendingPublish.result)
		switch {
		case err == nil:
			a.markPubsubDone(pendingPublish.prepared, messageID, addIDToDelete)
		case errors.Is(err, context.DeadlineExceeded):
			joined = errors.Join(joined, err)
			a.logPubsubFailure(ctx, pendingPublish.prepared, err)
		case isPubSubPermanentBackendError(err):
			done, isolateErr := a.sendPubsubIsolated(ctx, pendingPublish.prepared.evt, false, addIDToDelete)
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

func (a *app) sendPubsubOrderedGroup(ctx context.Context, events []event, addIDToDelete func(any)) error {
	for _, evt := range events {
		prepared, ok := a.preparePubsubEvent(evt, true, addIDToDelete)
		if !ok {
			continue
		}

		result := a.pubsub.Publish(ctx, prepared.message)
		a.pubsub.Flush(prepared.message.Topic)

		messageID, err := a.awaitPubsubResult(ctx, result)
		switch {
		case err == nil:
			a.markPubsubDone(prepared, messageID, addIDToDelete)
		case errors.Is(err, context.DeadlineExceeded):
			a.logPubsubFailure(ctx, prepared, err)
			return errors.Join(errFatalAfterCommit, err)
		case isPubSubPermanentBackendError(err):
			a.pubsub.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
			done, isolateErr := a.sendPubsubIsolated(ctx, evt, true, addIDToDelete)
			if isolateErr != nil {
				return isolateErr
			}
			if !done {
				return err
			}
		default:
			a.pubsub.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
			a.logPubsubFailure(ctx, prepared, err)
			return err
		}
	}
	return nil
}

func (a *app) sendPubsubIsolated(ctx context.Context, evt event, ordered bool, addIDToDelete func(any)) (bool, error) {
	prepared, ok := a.preparePubsubEvent(evt, ordered, addIDToDelete)
	if !ok {
		return true, nil
	}

	result := a.pubsub.Publish(ctx, prepared.message)
	a.pubsub.Flush(prepared.message.Topic)
	messageID, err := a.awaitPubsubResult(ctx, result)
	if err != nil && ordered && !errors.Is(err, context.DeadlineExceeded) {
		a.pubsub.ResumePublish(prepared.message.Topic, prepared.message.OrderingKey)
	}
	switch {
	case err == nil:
		a.markPubsubDone(prepared, messageID, addIDToDelete)
		return true, nil
	case errors.Is(err, context.DeadlineExceeded) && ordered:
		a.logPubsubFailure(ctx, prepared, err)
		return false, errors.Join(errFatalAfterCommit, err)
	case isPubSubPermanentBackendError(err):
		addIDToDelete(prepared.id)
		a.logPubsubFailure(ctx, prepared, err)
		return true, nil
	default:
		a.logPubsubFailure(ctx, prepared, err)
		return false, err
	}
}

type pubsubPreparedEvent struct {
	evt        event
	id         any
	timestamp  any
	latency    any
	target     string
	message    pubsubMessage
	startedAt  time.Time
	payloadLen int
}

type pubsubPendingPublish struct {
	prepared pubsubPreparedEvent
	result   pubsubPublishResult
}

func (a *app) preparePubsubEvent(evt event, ordered bool, addIDToDelete func(any)) (pubsubPreparedEvent, bool) {
	target := eventOptionalString(evt, a.cfg.EventTarget)
	topicName := a.destinationForBackend(evt, backendPubSub)
	orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
	if !ordered {
		orderingKey = ""
	}
	attributes := eventAttributes(evt, a.cfg.EventAttributes)
	timestamp := eventValue(evt, a.cfg.EventTimestamp)
	id := eventValue(evt, a.cfg.EventID)
	data := eventBytes(evt, a.cfg.EventPayload)
	latency := eventLatency(timestamp)

	stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
	if len(deletedAttributes) != 0 {
		slog.Error("Some attributes were dropped",
			"event_id", id,
			"event_destination", topicName,
			"dropped_attributes", deletedAttributes,
		)
	}

	prepared := pubsubPreparedEvent{
		evt:        evt,
		id:         id,
		timestamp:  timestamp,
		latency:    latency,
		target:     target,
		startedAt:  time.Now(),
		payloadLen: len(data),
		message: pubsubMessage{
			Topic:       topicName,
			Data:        data,
			OrderingKey: orderingKey,
			Attributes:  stringAttributes,
		},
	}

	if reason, poison := pubsubPoisonReason(prepared.message); poison {
		addIDToDelete(id)
		slog.Error("Failed to send event",
			"event_id", id,
			"event_destination", topicName,
			"error", reason,
		)
		return prepared, false
	}

	slog.Debug("Sending event",
		"event_id", id,
		"event_timestamp", timestamp,
		"event_latency", latency,
		"event_payload_size", len(data),
		"event_ordering_key", orderingKey,
		"event_attributes", stringAttributes,
		"event_target", target,
		"event_destination", topicName,
	)

	return prepared, true
}

func (a *app) markPubsubDone(prepared pubsubPreparedEvent, messageID string, addIDToDelete func(any)) {
	addIDToDelete(prepared.id)
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

func (a *app) logPubsubFailure(ctx context.Context, prepared pubsubPreparedEvent, err error) {
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

func (a *app) awaitPubsubResult(ctx context.Context, result pubsubPublishResult) (string, error) {
	waitCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout+a.cfg.PublishResultGrace)
	defer cancel()
	return result.Get(waitCtx)
}

func pubsubPoisonReason(message pubsubMessage) (string, bool) {
	if len(message.Data) > pubsubMaxMessageDataBytes {
		return "Pub/Sub message data exceeds 10 MB", true
	}
	if len(message.Data) == 0 && len(message.Attributes) == 0 && message.OrderingKey == "" {
		return "Pub/Sub message has no data, attributes, or ordering key", true
	}
	if !validPubSubAttributes(message.Attributes) {
		return "Pub/Sub attributes exceed provider limits", true
	}
	if !validPubSubTopic(message.Topic) {
		return "Pub/Sub topic name is syntactically invalid", true
	}
	return "", false
}

func validPubSubAttributes(attributes map[string]string) bool {
	if len(attributes) > pubsubMaxAttributes {
		return false
	}
	for key, value := range attributes {
		if key == "" {
			return false
		}
		if len([]byte(key)) > pubsubMaxAttributeKeyBytes || len([]byte(value)) > pubsubMaxAttributeValueBytes {
			return false
		}
		if strings.HasPrefix(strings.ToLower(key), "goog") {
			return false
		}
	}
	return true
}

func validPubSubTopic(topic string) bool {
	parts := strings.Split(topic, "/")
	if len(parts) == 4 {
		return parts[0] == "projects" && parts[1] != "" && parts[2] == "topics" && validPubSubTopicID(parts[3])
	}
	if strings.Contains(topic, "/") {
		return false
	}
	return validPubSubTopicID(topic)
}

func validPubSubTopicID(topicID string) bool {
	return !strings.HasPrefix(strings.ToLower(topicID), "goog") && pubsubTopicIDPattern.MatchString(topicID)
}

func isPubSubPermanentBackendError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == pubsubPermanentBackendErrorCode {
		return true
	}

	code := status.Code(err)
	return code == codes.InvalidArgument || code == codes.OutOfRange
}

// sendPubsubEvent is kept as a compatibility wrapper for narrow unit tests.
func (a *app) sendPubsubEvent(ctx context.Context, tx *sql.Tx, evt event, addIDToDelete func(any)) error {
	return a.sendPubsubEvents(ctx, tx, []event{evt}, addIDToDelete)
}

func pubsubPermanentError(reason string) error {
	return &googleapi.Error{Code: pubsubPermanentBackendErrorCode, Message: fmt.Sprintf("permanent Pub/Sub rejection: %s", reason)}
}
