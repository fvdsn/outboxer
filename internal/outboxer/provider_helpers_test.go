package outboxer

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/api/googleapi"
)

// These helpers keep provider tests focused on publishing behavior while the
// production processor owns route resolution and sender outcome collection.

func (a *app) sendPubsubEventsForTest(ctx context.Context, events []event, addIDToDelete func(any)) error {
	return a.sendPubsubEventsWithCallbacks(ctx, a.routeTestEvents(events, backendPubSub), senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
}

func (a *app) sendPubsubEventForTest(ctx context.Context, evt event, addIDToDelete func(any)) error {
	return a.sendPubsubEventsForTest(ctx, []event{evt}, addIDToDelete)
}

func pubsubPermanentError(reason string) error {
	return &googleapi.Error{Code: pubsubPermanentBackendErrorCode, Message: fmt.Sprintf("permanent Pub/Sub rejection: %s", reason)}
}

func (a *app) sendSQSEventsForTest(ctx context.Context, events []event, addIDToDelete func(any)) error {
	return a.sendSQSEventsWithCallbacks(ctx, a.routeTestEvents(events, backendSQS), senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
}

func (a *app) sendSQSBatchForTest(ctx context.Context, queueURL string, events []event, addIDToDelete func(any)) error {
	events = a.routeTestEvents(events, backendSQS)
	for i := range events {
		events[i].route.destination = queueURL
	}
	callbacks := senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	}
	if !validSQSQueueURL(queueURL) {
		return a.sendSQSQueueEvents(ctx, make(chan struct{}, a.cfg.SQSSendConcurrency), queueURL, events, callbacks)
	}
	prepared := make([]sqsPreparedEvent, 0, len(events))
	for _, evt := range events {
		candidate, ok := a.parseSQSCandidate(ctx, evt, queueURL, callbacks)
		if !ok {
			continue
		}
		item, ok := a.prepareSQSEvent(ctx, candidate, queueURL, strings.HasSuffix(queueURL, ".fifo"), callbacks)
		if ok {
			prepared = append(prepared, item)
		}
	}
	_, err := a.sendSQSBatch(ctx, queueURL, prepared, callbacks)
	return err
}

func (a *app) routeTestEvents(events []event, selected backend) []event {
	routed := make([]event, len(events))
	for i, evt := range events {
		destination := eventString(evt, a.cfg.EventDestination)
		if destination == "" {
			if selected == backendPubSub {
				destination = a.cfg.DefaultPubSubTopic
			} else {
				destination = a.cfg.DefaultSQSQueueURL
			}
		}
		evt.route = eventRoute{backend: selected, destination: destination}
		routed[i] = evt
	}
	return routed
}
