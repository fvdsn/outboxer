package outboxer

import (
	"context"
	"database/sql"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type pubsubPublisher interface {
	Publish(ctx context.Context, message pubsubMessage) (string, error)
}

type pubsubMessage struct {
	Topic       string
	Data        []byte
	OrderingKey string
	Attributes  map[string]string
}

type cloudPubSubPublisher struct {
	client *pubsub.Client
}

func newPubSubClient(ctx context.Context, cfg appConfig) (*pubsub.Client, error) {
	options := []option.ClientOption{}
	if cfg.PubSubAPIEndpoint != "" {
		options = append(options, option.WithEndpoint(cfg.PubSubAPIEndpoint))
	}
	return pubsub.NewClient(ctx, "", options...)
}

func (p *cloudPubSubPublisher) Publish(ctx context.Context, message pubsubMessage) (string, error) {
	topic := p.client.Topic(message.Topic)
	if message.OrderingKey != "" {
		topic.EnableMessageOrdering = true
	}

	pubsubMsg := &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	}
	if message.OrderingKey != "" {
		pubsubMsg.OrderingKey = message.OrderingKey
	}

	return topic.Publish(ctx, pubsubMsg).Get(ctx)
}

func (a *app) sendPubsubEvents(ctx context.Context, tx *sql.Tx, events []event, addIDToDelete func(any)) error {
	for _, evt := range events {
		if err := a.sendPubsubEvent(ctx, tx, evt, addIDToDelete); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) sendPubsubEvent(ctx context.Context, tx *sql.Tx, evt event, addIDToDelete func(any)) error {
	target := eventOptionalString(evt, a.cfg.EventTarget)
	topicName := eventString(evt, a.cfg.EventDestination)
	if topicName == "" {
		topicName = a.cfg.DefaultPubSubTopic
	}
	orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
	attributes := eventAttributes(evt, a.cfg.EventAttributes)
	timestamp := eventValue(evt, a.cfg.EventTimestamp)
	id := eventValue(evt, a.cfg.EventID)
	data := eventBytes(evt, a.cfg.EventPayload)
	latency := eventLatency(timestamp)

	logDebug(map[string]any{
		"message":          "Sending event",
		"eventId":          id,
		"eventTimestamp":   timestamp,
		"eventLatency":     latency,
		"eventPayloadSize": len(data),
		"eventOrderingKey": orderingKey,
		"eventAttributes":  attributes,
		"eventTarget":      target,
		"eventDestination": topicName,
	})

	start := time.Now()
	stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
	if len(deletedAttributes) != 0 {
		logError(map[string]any{
			"message":           "Some attributes were deleted",
			"eventId":           id,
			"eventDestination":  topicName,
			"deletedAttributes": deletedAttributes,
		})
	}

	messageID, err := a.pubsub.Publish(ctx, pubsubMessage{
		Topic:       topicName,
		Data:        data,
		OrderingKey: orderingKey,
		Attributes:  stringAttributes,
	})
	if err != nil {
		logError(map[string]any{
			"message":          "Failed to send event",
			"eventId":          id,
			"eventOrderingKey": orderingKey,
			"eventAttributes":  stringAttributes,
			"eventTarget":      target,
			"eventDestination": topicName,
			"error":            err.Error(),
		})
		return err
	}

	publishLatency := time.Since(start).Seconds()
	if orderingKey != "" {
		a.txMu.Lock()
		err = a.deleteEvent(ctx, tx, id)
		a.txMu.Unlock()
		if err != nil {
			return err
		}
	} else {
		addIDToDelete(id)
	}

	logDebug(map[string]any{
		"message":          "Event sent",
		"eventId":          id,
		"eventTimestamp":   timestamp,
		"eventLatency":     latency,
		"eventPayloadSize": len(data),
		"eventPublishedId": messageID,
		"eventOrderingKey": orderingKey,
		"eventAttributes":  stringAttributes,
		"eventTarget":      target,
		"eventDestination": topicName,
		"publishLatency":   publishLatency,
	})

	return nil
}
