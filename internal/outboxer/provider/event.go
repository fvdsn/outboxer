package provider

import (
	"errors"
	"fmt"
	"time"
)

// ErrMalformedOptions identifies invalid provider-specific event options.
var ErrMalformedOptions = errors.New("malformed event options")

// EventID is an outbox row's identifier as its raw database value (int64 for a
// bigint column, string for uuid/text, and so on). It is opaque to providers:
// report it back through the callbacks exactly as received, without converting
// or inspecting it, so the relay can match the outcome to its row.
type EventID = any

// Event is the provider-facing view of a selected outbox row. The relay core
// resolves each configured column into a role before dispatch, so providers
// never deal with raw database values or column names: Destination is the route
// resolved by the collection query, Timestamp is zero when the event has none,
// and Options is this provider's already-parsed section of the options column.
type Event struct {
	ID          EventID
	Payload     []byte
	Timestamp   time.Time
	Destination string
	Options     Options
}

// Latency returns the number of seconds elapsed since the event's timestamp,
// measured at the moment of the call, or nil when the event has no timestamp.
func (e Event) Latency() any {
	if e.Timestamp.IsZero() {
		return nil
	}
	return time.Since(e.Timestamp).Seconds()
}

// Options is one provider's section of the event options object, already parsed
// and extracted by the relay core.
type Options struct {
	Values map[string]any
}

// String returns an option as a string, erroring if it is present but not one.
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

// Object returns an option as an object, erroring if it is present but not one.
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
