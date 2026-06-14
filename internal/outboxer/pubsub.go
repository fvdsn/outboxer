package outboxer

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
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
	publisher := p.client.Publisher(message.Topic)
	if message.OrderingKey != "" {
		publisher.EnableMessageOrdering = true
	}
	defer publisher.Stop()

	pubsubMsg := &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	}
	if message.OrderingKey != "" {
		pubsubMsg.OrderingKey = message.OrderingKey
	}

	return publisher.Publish(ctx, pubsubMsg).Get(ctx)
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

	slog.Debug("Sending event",
		"event_id", id,
		"event_timestamp", timestamp,
		"event_latency", latency,
		"event_payload_size", len(data),
		"event_ordering_key", orderingKey,
		"event_attributes", attributes,
		"event_target", target,
		"event_destination", topicName,
	)

	start := time.Now()
	stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
	if len(deletedAttributes) != 0 {
		slog.Error("Some attributes were dropped",
			"event_id", id,
			"event_destination", topicName,
			"dropped_attributes", deletedAttributes,
		)
	}

	publishCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout)
	defer cancel()
	messageID, err := a.pubsub.Publish(publishCtx, pubsubMessage{
		Topic:       topicName,
		Data:        data,
		OrderingKey: orderingKey,
		Attributes:  stringAttributes,
	})
	if err != nil {
		slog.Error("Failed to send event",
			"event_id", id,
			"event_ordering_key", orderingKey,
			"event_attributes", stringAttributes,
			"event_target", target,
			"event_destination", topicName,
			"error", err.Error(),
		)
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

	slog.Debug("Event sent",
		"event_id", id,
		"event_timestamp", timestamp,
		"event_latency", latency,
		"event_payload_size", len(data),
		"event_published_id", messageID,
		"event_ordering_key", orderingKey,
		"event_attributes", stringAttributes,
		"event_target", target,
		"event_destination", topicName,
		"publish_latency", publishLatency,
	)

	return nil
}
