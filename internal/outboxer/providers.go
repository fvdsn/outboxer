package outboxer

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/fvdsn/outboxer/internal/outboxer/provider"
	outboxpubsub "github.com/fvdsn/outboxer/internal/outboxer/pubsub"
	outboxsqs "github.com/fvdsn/outboxer/internal/outboxer/sqs"
)

const awsWebIdentityProviderGoogle = outboxsqs.WebIdentityProviderGoogle

type pubsubPublisher = outboxpubsub.Publisher
type pubsubPublishResult = outboxpubsub.PublishResult
type pubsubMessage = outboxpubsub.Message
type sqsPublisher = outboxsqs.Publisher
type sqsBatchEntry = outboxsqs.BatchEntry
type sqsBatchResponse = outboxsqs.BatchResponse
type sqsBatchSuccess = outboxsqs.BatchSuccess
type sqsBatchFailure = outboxsqs.BatchFailure
type sqsMessageAttribute = outboxsqs.MessageAttribute

func pubsubConfig(cfg appConfig) outboxpubsub.Config {
	return outboxpubsub.Config{
		EventID:            cfg.EventID,
		EventTimestamp:     cfg.EventTimestamp,
		EventPayload:       cfg.EventPayload,
		EventTarget:        cfg.EventTarget,
		EventOptions:       cfg.EventOptions,
		PubSubProjectID:    cfg.PubSubProjectID,
		PubSubAPIEndpoint:  cfg.PubSubAPIEndpoint,
		PublishTimeout:     cfg.PublishTimeout,
		PublishResultGrace: cfg.PublishResultGrace,
	}
}

func newPubSubClient(ctx context.Context, cfg appConfig) (*pubsub.Client, error) {
	return outboxpubsub.NewClient(ctx, pubsubConfig(cfg))
}

func newCloudPubSubPublisher(client *pubsub.Client, cfg appConfig) outboxpubsub.Publisher {
	return outboxpubsub.NewCloudPublisher(client, pubsubConfig(cfg))
}

func (a *app) sendPubsubEventsWithCallbacks(ctx context.Context, events []event, callbacks senderCallbacks) error {
	return outboxpubsub.Send(ctx, pubsubConfig(a.cfg), a.pubsub, providerEvents(events), outboxpubsub.Callbacks{
		AddConfirmedID: callbacks.addConfirmedID,
		AddPoisonID:    callbacks.addPoisonID,
		MarkProgress:   markProcessorProgress,
		LogFailure:     a.logFailure,
	})
}

func sqsConfig(cfg appConfig) outboxsqs.Config {
	return outboxsqs.Config{
		EventID:                    cfg.EventID,
		EventTimestamp:             cfg.EventTimestamp,
		EventPayload:               cfg.EventPayload,
		EventOptions:               cfg.EventOptions,
		SQSSendConcurrency:         cfg.SQSSendConcurrency,
		PublishTimeout:             cfg.PublishTimeout,
		SQSAPIEndpoint:             cfg.SQSAPIEndpoint,
		AWSRegion:                  cfg.AWSRegion,
		AWSRoleARN:                 cfg.AWSRoleARN,
		AWSRoleSessionName:         cfg.AWSRoleSessionName,
		AWSRoleDuration:            cfg.AWSRoleDuration,
		AWSCredentialRefreshWindow: cfg.AWSCredentialRefreshWindow,
		AWSWebIdentityProvider:     cfg.AWSWebIdentityProvider,
		AWSWebIdentityAudience:     cfg.AWSWebIdentityAudience,
	}
}

func newSQSClient(ctx context.Context, cfg appConfig) (*awssqs.Client, error) {
	return outboxsqs.NewClient(ctx, sqsConfig(cfg))
}

func newAWSSQSPublisher(client *awssqs.Client) outboxsqs.Publisher {
	return outboxsqs.NewPublisher(client)
}

func (a *app) sendSQSEventsWithCallbacks(ctx context.Context, events []event, callbacks senderCallbacks) error {
	return outboxsqs.Send(ctx, sqsConfig(a.cfg), a.sqs, providerEvents(events), outboxsqs.Callbacks{
		AddConfirmedID: callbacks.addConfirmedID,
		AddPoisonID:    callbacks.addPoisonID,
		MarkProgress:   markProcessorProgress,
		LogFailure:     a.logFailure,
	})
}

func providerEvents(events []event) []provider.Event {
	converted := make([]provider.Event, len(events))
	for i, evt := range events {
		converted[i] = provider.Event{
			Columns:     evt.columns,
			Destination: evt.route.destination,
		}
	}
	return converted
}
