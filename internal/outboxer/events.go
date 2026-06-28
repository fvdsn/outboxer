package outboxer

import (
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

var errMalformedOptions = provider.ErrMalformedOptions

const (
	eventTargetPubSub = "pubsub"
	eventTargetSQS    = "sqs"
)

type backend int

const (
	backendNone backend = iota
	backendPubSub
	backendSQS
)

type eventRoute struct {
	// The selection query resolves defaults and ownership filters. Senders use
	// this route directly so dispatch cannot disagree with collection.
	backend     backend
	destination string
}

type event struct {
	columns map[string]any
	route   eventRoute
}

type backendOptions = provider.Options

func providerEvent(evt event) provider.Event {
	return provider.Event{
		Columns:     evt.columns,
		Destination: evt.route.destination,
	}
}

func eventValue(evt event, column string) any {
	return provider.Value(providerEvent(evt), column)
}

func eventString(evt event, column string) string {
	return provider.String(providerEvent(evt), column)
}

func valueString(value any) string {
	return provider.ValueString(value)
}

func eventPubSubOptions(evt event, cfg appConfig) (backendOptions, error) {
	return provider.BackendOptions(providerEvent(evt), cfg.EventOptions, eventTargetPubSub)
}

func eventSQSOptions(evt event, cfg appConfig) (backendOptions, error) {
	return provider.BackendOptions(providerEvent(evt), cfg.EventOptions, eventTargetSQS)
}

func eventTimestamp(value any) (time.Time, bool) {
	return provider.Timestamp(value)
}
