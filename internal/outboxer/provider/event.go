package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrMalformedOptions identifies invalid provider-specific event options.
var ErrMalformedOptions = errors.New("malformed event options")

// Event is the provider-facing view of a selected outbox row. The relay core
// resolves each configured column into a role before dispatch, so providers
// never deal with raw database values or column names; Destination is the route
// already resolved by the collection query, Options is the raw value of the
// options column, and Timestamp is zero when the event has no timestamp.
type Event struct {
	ID          any
	Payload     []byte
	Timestamp   time.Time
	Destination string
	Options     any
}

// Options contains one provider's section of the event options object.
type Options struct {
	Values map[string]any
}

// BackendOptions extracts and validates one provider's options section from the
// raw options column value.
func BackendOptions(options any, backend string) (Options, error) {
	root, err := optionsObject(options)
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

// Latency returns the number of seconds elapsed since the event's timestamp,
// measured at the moment of the call, or nil when the event has no timestamp.
func Latency(timestamp time.Time) any {
	if timestamp.IsZero() {
		return nil
	}
	return time.Since(timestamp).Seconds()
}

func optionsObject(value any) (map[string]any, error) {
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
