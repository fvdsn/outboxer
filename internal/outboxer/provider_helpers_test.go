package outboxer

import (
	"context"
)

// These helpers keep provider tests focused on publishing behavior while the
// production processor owns route resolution and sender outcome collection.

func (a *app) sendPubsubEventsForTest(ctx context.Context, events []event, addIDToDelete func(any)) error {
	return a.sendPubsubEventsWithCallbacks(ctx, a.routeTestPubsubEvents(events), senderCallbacks{
		addConfirmedID: addIDToDelete,
		addPoisonID: func(id any, _ string) {
			addIDToDelete(id)
		},
	})
}

func (a *app) sendPubsubEventForTest(ctx context.Context, evt event, addIDToDelete func(any)) error {
	return a.sendPubsubEventsForTest(ctx, []event{evt}, addIDToDelete)
}

func (a *app) routeTestPubsubEvents(events []event) []event {
	routed := make([]event, len(events))
	for i, evt := range events {
		destination := eventString(evt, a.cfg.EventDestination)
		if destination == "" {
			destination = a.cfg.DefaultPubSubTopic
		}
		evt.route = eventRoute{backend: backendPubSub, destination: destination}
		routed[i] = evt
	}
	return routed
}
