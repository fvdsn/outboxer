package outboxer

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/api/googleapi"
)

// This file holds compatibility wrappers used only by tests. They adapt the
// real senderCallbacks-based publishing API to the older single-callback shape
// some unit tests were written against, and construct provider errors for those
// tests. Keeping them in a _test.go file keeps the production binary free of
// test-only scaffolding.

func (a *app) resolveBackend(evt event) backend {
	return a.classifyRoute(evt).backend
}

func (a *app) sendPubsubEvents(ctx context.Context, events []event, addIDToDelete func(any)) error {
	return a.sendPubsubEventsWithCallbacks(ctx, events, senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
}

func (a *app) sendPubsubEvent(ctx context.Context, evt event, addIDToDelete func(any)) error {
	return a.sendPubsubEvents(ctx, []event{evt}, addIDToDelete)
}

func pubsubPermanentError(reason string) error {
	return &googleapi.Error{Code: pubsubPermanentBackendErrorCode, Message: fmt.Sprintf("permanent Pub/Sub rejection: %s", reason)}
}

func (a *app) sendSQSEvents(ctx context.Context, events []event, addIDToDelete func(any)) error {
	return a.sendSQSEventsWithCallbacks(ctx, events, senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
}

func (a *app) sendSQS10Events(ctx context.Context, queueURL string, events []event, addIDToDelete func(any)) error {
	_, err := a.sendSQSBatch(ctx, queueURL, events, strings.HasSuffix(queueURL, ".fifo"), senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
	return err
}
