package provider

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrFatalAfterCommit tells the relay to exit after committing completed work.
var ErrFatalAfterCommit = errors.New("fatal after commit")

// Sender publishes a batch of events for one registered target.
type Sender interface {
	Send(ctx context.Context, events []Event, callbacks Callbacks) error
}

// Callbacks reports per-event outcomes and publishing progress to the relay.
type Callbacks struct {
	AddConfirmedID func(EventID)
	AddPoisonID    func(EventID, string)
	MarkProgress   func()
	LogFailure     func(context.Context, string, string, ...any)
}

// Progress marks publishing liveness for the relay watchdog, when wired.
func (c Callbacks) Progress() {
	if c.MarkProgress != nil {
		c.MarkProgress()
	}
}

// ReportFailure forwards a publish failure to the relay's rate-limited failure
// logger, when wired. The signature groups repeats of the same failure so the
// logger can suppress them.
func (c Callbacks) ReportFailure(ctx context.Context, message string, signature string, attrs ...any) {
	if c.LogFailure != nil {
		c.LogFailure(ctx, message, signature, attrs...)
	}
}

// RejectMalformedOptions poisons an event whose options section is structurally
// invalid and reports the rejection, keyed by the offending field.
func (c Callbacks) RejectMalformedOptions(ctx context.Context, target string, evt Event, field string, err error) {
	c.AddPoisonID(evt.ID, err.Error())
	c.ReportFailure(ctx, "Failed to send event",
		fmt.Sprintf("%s|%s|%s|malformed-options", target, evt.Destination, field),
		"event_id", evt.ID,
		"event_destination", evt.Destination,
		"error", err.Error(),
	)
}

// WithTimeout derives a context with the given timeout. A non-positive timeout
// disables the deadline and returns the parent context unchanged.
func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
