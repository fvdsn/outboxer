package provider

import (
	"log/slog"
	"time"
)

// PublishLog carries the identifying fields a provider logs for each publish,
// so the "Sending event" and "Event sent" debug lines stay consistent across
// targets.
type PublishLog struct {
	ID          EventID
	Timestamp   any
	Latency     any
	PayloadSize int
	OrderingKey string
	Attributes  any
	Target      string
	Destination string
}

// NewPublishLog captures the shared logging fields of one event; the caller
// fills in its provider-specific ordering key and attributes.
func NewPublishLog(evt Event, target string) PublishLog {
	log := PublishLog{
		ID:          evt.ID,
		PayloadSize: len(evt.Payload),
		Target:      target,
		Destination: evt.Destination,
	}
	// An absent timestamp logs as nil rather than the zero time.
	if !evt.Timestamp.IsZero() {
		log.Timestamp = evt.Timestamp
		log.Latency = time.Since(evt.Timestamp).Seconds()
	}
	return log
}

func (l PublishLog) attrs() []any {
	return []any{
		"event_id", l.ID,
		"event_timestamp", l.Timestamp,
		"event_latency", l.Latency,
		"event_payload_size", l.PayloadSize,
		"event_ordering_key", l.OrderingKey,
		"event_attributes", l.Attributes,
		"event_target", l.Target,
		"event_destination", l.Destination,
	}
}

// Sending logs the pre-publish debug line.
func (l PublishLog) Sending() {
	slog.Debug("Sending event", l.attrs()...)
}

// Sent logs the post-publish confirmation debug line.
func (l PublishLog) Sent(publishedID string, publishLatency float64) {
	slog.Debug("Event sent", append(l.attrs(),
		"event_published_id", publishedID,
		"publish_latency", publishLatency,
	)...)
}
