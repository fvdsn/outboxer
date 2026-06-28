package provider

import (
	"context"
	"errors"
)

// ErrFatalAfterCommit tells the relay to exit after committing completed work.
var ErrFatalAfterCommit = errors.New("fatal after commit")

// Sender publishes a batch of events for one registered target.
type Sender interface {
	Send(ctx context.Context, events []Event, callbacks Callbacks) error
}

// Callbacks reports per-event outcomes and publishing progress to the relay.
type Callbacks struct {
	AddConfirmedID func(any)
	AddPoisonID    func(any, string)
	MarkProgress   func()
	LogFailure     func(context.Context, string, string, ...any)
}
