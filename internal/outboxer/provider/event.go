package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrMalformedOptions identifies invalid provider-specific event options.
var ErrMalformedOptions = errors.New("malformed event options")

// Event is the provider-facing view of a selected outbox row. Columns remain
// keyed by their configured database names; Destination is the route already
// resolved by the collection query.
type Event struct {
	Columns     map[string]any
	Destination string
}

// Value returns the raw value of a configured event column.
func Value(evt Event, column string) any {
	return evt.Columns[column]
}

// String returns an event column as a string.
func String(evt Event, column string) string {
	return ValueString(Value(evt, column))
}

// ValueString converts a database value to its string representation.
func ValueString(value any) string {
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

// Bytes returns an event column as bytes.
func Bytes(evt Event, column string) []byte {
	value := Value(evt, column)
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

// Options contains one provider's section of the event options object.
type Options struct {
	Values map[string]any
}

// BackendOptions extracts and validates one provider's options section.
func BackendOptions(evt Event, column string, backend string) (Options, error) {
	root, err := optionsObject(evt, column)
	if err != nil {
		return Options{}, err
	}
	value, ok := root[backend]
	if !ok || value == nil {
		return Options{}, nil
	}
	section, ok := Object(value)
	if !ok {
		return Options{}, fmt.Errorf("%w: %s section must be an object", ErrMalformedOptions, backend)
	}
	return Options{Values: section}, nil
}

func (o Options) String(key string) (string, error) {
	value, ok := o.Values[key]
	if !ok || value == nil {
		return "", nil
	}
	stringValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%w: %s must be a string", ErrMalformedOptions, key)
	}
	return stringValue, nil
}

// Object returns an option value as an object.
func (o Options) Object(key string) (map[string]any, error) {
	value, ok := o.Values[key]
	if !ok || value == nil {
		return nil, nil
	}
	object, ok := Object(value)
	if !ok {
		return nil, fmt.Errorf("%w: %s must be an object", ErrMalformedOptions, key)
	}
	return object, nil
}

// Object performs a checked conversion to a string-keyed object.
func Object(value any) (map[string]any, bool) {
	object, ok := value.(map[string]any)
	return object, ok
}

// Latency returns the number of seconds since a timestamp-like value.
func Latency(value any) any {
	timestamp, ok := Timestamp(value)
	if !ok {
		return nil
	}
	return time.Since(timestamp).Seconds()
}

// Timestamp parses a supported database timestamp value and normalizes it to UTC.
func Timestamp(value any) (time.Time, bool) {
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

func optionsObject(evt Event, column string) (map[string]any, error) {
	if column == "" {
		return nil, nil
	}
	value := Value(evt, column)
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
		return nil, fmt.Errorf("%w: options column must be an object", ErrMalformedOptions)
	}
}

func parseOptionsJSON(content []byte) (map[string]any, error) {
	if len(content) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(content, &decoded); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformedOptions, err)
	}
	if decoded == nil {
		return nil, nil
	}
	options, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: options column must be an object", ErrMalformedOptions)
	}
	return options, nil
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
