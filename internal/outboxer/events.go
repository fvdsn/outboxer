package outboxer

import (
	"fmt"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

type eventRoute struct {
	// The selection query resolves defaults and ownership filters. Senders use
	// this route directly so dispatch cannot disagree with collection.
	target      string
	destination string
}

type event struct {
	columns map[string]any
	route   eventRoute
}

func providerEvent(evt event, cfg appConfig) provider.Event {
	timestamp, _ := eventTimestamp(evt.columns[cfg.EventTimestamp])
	return provider.Event{
		ID:          evt.columns[cfg.EventID],
		Payload:     valueBytes(evt.columns[cfg.EventPayload]),
		Timestamp:   timestamp,
		Destination: evt.route.destination,
		Options:     evt.columns[cfg.EventOptions],
	}
}

func eventValue(evt event, column string) any {
	return evt.columns[column]
}

func eventString(evt event, column string) string {
	return valueString(evt.columns[column])
}

// valueString converts a raw database/sql value to its string representation.
func valueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(typed)
	}
}

// valueBytes converts a raw database/sql value to its byte representation.
func valueBytes(value any) []byte {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		return []byte(fmt.Sprint(typed))
	}
}

// eventTimestamp parses a supported database timestamp value and normalizes it
// to UTC. The bool is false when the value is absent or unparseable.
func eventTimestamp(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case nil:
		return time.Time{}, false
	case time.Time:
		return typed.UTC(), true
	case string:
		return parseTimestampString(typed)
	case []byte:
		return parseTimestampString(string(typed))
	default:
		return time.Time{}, false
	}
}

func parseTimestampString(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), true
	}
	if parsed, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", value, time.UTC); err == nil {
		return parsed.UTC(), true
	}
	if parsed, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.UTC); err == nil {
		return parsed.UTC(), true
	}
	return time.Time{}, false
}
