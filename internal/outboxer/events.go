package outboxer

import (
	"encoding/json"
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
	// Structurally malformed options are dead-lettered before dispatch (see
	// processEventBatch), so dispatched events always carry a valid section.
	options, _ := eventOptions(evt.columns[cfg.EventOptions], evt.route.target)
	return provider.Event{
		ID:          evt.columns[cfg.EventID],
		Payload:     valueBytes(evt.columns[cfg.EventPayload]),
		Timestamp:   timestamp,
		Destination: evt.route.destination,
		Options:     options,
	}
}

// eventOptions parses the raw options column and extracts the section belonging
// to target. It errors only on structural problems (invalid JSON, a non-object
// root, or a non-object section); per-field validation is each provider's job.
func eventOptions(raw any, target string) (provider.Options, error) {
	root, err := optionsRoot(raw)
	if err != nil {
		return provider.Options{}, err
	}
	section, ok := root[target]
	if !ok || section == nil {
		return provider.Options{}, nil
	}
	values, ok := provider.Object(section)
	if !ok {
		return provider.Options{}, fmt.Errorf("%w: %s section must be an object", provider.ErrMalformedOptions, target)
	}
	return provider.Options{Values: values}, nil
}

func optionsRoot(value any) (map[string]any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		return typed, nil
	case []byte:
		return parseOptionsJSON(typed)
	case string:
		return parseOptionsJSON([]byte(typed))
	default:
		return nil, fmt.Errorf("%w: options column must be an object", provider.ErrMalformedOptions)
	}
}

func parseOptionsJSON(content []byte) (map[string]any, error) {
	if len(content) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(content, &decoded); err != nil {
		return nil, fmt.Errorf("%w: %w", provider.ErrMalformedOptions, err)
	}
	if decoded == nil {
		return nil, nil
	}
	options, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: options column must be an object", provider.ErrMalformedOptions)
	}
	return options, nil
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
