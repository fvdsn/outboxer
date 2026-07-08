package harness

import (
	"context"
	"testing"
	"time"
)

// MessageSink abstracts where relayed events land, so the scenarios run
// identically against Pub/Sub subscriptions and SQS queues.
type MessageSink interface {
	// Receive pulls until want messages whose payload carries the prefix
	// arrived or the timeout passed, acking everything as it goes (stale runs
	// and settle canaries are dropped by the filter). Fails the test on
	// shortfall.
	Receive(ctx context.Context, t *testing.T, prefix string, want int, timeout time.Duration) []ReceivedMessage
	// Stream delivers messages on a channel until stop is called. Used by the
	// latency scenario, where receiver startup must not count as latency.
	Stream(ctx context.Context, t *testing.T) (<-chan ReceivedMessage, func())
	// Purge drops everything currently held without delivering it.
	Purge(ctx context.Context, t *testing.T)
}

// SmokeEvents builds the backend-specific events for the smoke scenario: the
// same logical mix (unordered, ordered per key, poison) maps to different
// destinations and options per provider. The scenario supplies run-unique
// payloads; builders supply routing.
type SmokeEvents struct {
	// Unordered returns event i of the unordered batch; implementations
	// should alternate between the default and an explicit destination.
	Unordered func(payload string, i int) Event
	// Ordered returns event i of the ordered sequence for key.
	Ordered func(payload string, key string, i int) Event
	// Poison returns an event the relay must dead-letter.
	Poison func(payload string, i int) Event
}
