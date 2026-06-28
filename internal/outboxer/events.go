package outboxer

import (
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
	return provider.Event{
		ID:          evt.columns[cfg.EventID],
		Payload:     provider.ValueBytes(evt.columns[cfg.EventPayload]),
		Timestamp:   evt.columns[cfg.EventTimestamp],
		Destination: evt.route.destination,
		Options:     evt.columns[cfg.EventOptions],
	}
}

func eventValue(evt event, column string) any {
	return evt.columns[column]
}

func eventString(evt event, column string) string {
	return provider.ValueString(evt.columns[column])
}

func valueString(value any) string {
	return provider.ValueString(value)
}

func eventTimestamp(value any) (time.Time, bool) {
	return provider.Timestamp(value)
}
