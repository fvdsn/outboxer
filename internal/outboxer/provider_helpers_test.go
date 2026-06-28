package outboxer

import (
	"context"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
	outboxpubsub "github.com/fvdsn/outboxer/internal/outboxer/pubsub"
	outboxsqs "github.com/fvdsn/outboxer/internal/outboxer/sqs"
)

// These helpers keep provider tests focused on publishing behavior while the
// production processor owns route resolution and sender outcome collection.

func (a *app) sendPubsubEventsForTest(ctx context.Context, events []event, addIDToDelete func(any)) error {
	sender := a.senders[eventTargetPubSub]
	return sender.Send(ctx, providerEvents(a.routeTestPubsubEvents(events), a.cfg), provider.Callbacks{
		AddConfirmedID: addIDToDelete,
		AddPoisonID: func(id any, _ string) {
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
		evt.route = eventRoute{target: eventTargetPubSub, destination: destination}
		routed[i] = evt
	}
	return routed
}

func setTestPubSubProvider(a *app, publisher outboxpubsub.Publisher) {
	if a.senders == nil {
		a.senders = map[string]provider.Sender{}
	}
	a.senders[eventTargetPubSub] = outboxpubsub.NewSender(pubsubConfig(a.cfg), publisher)
}

func setTestSQSProvider(a *app, publisher outboxsqs.Publisher) {
	if a.senders == nil {
		a.senders = map[string]provider.Sender{}
	}
	a.senders[eventTargetSQS] = outboxsqs.NewSender(sqsConfig(a.cfg), publisher)
}

type recordingProviderSender struct {
	events []provider.Event
}

func (s *recordingProviderSender) Send(_ context.Context, events []provider.Event, callbacks provider.Callbacks) error {
	s.events = append(s.events, events...)
	for _, evt := range events {
		callbacks.AddConfirmedID(evt.ID)
	}
	return nil
}
