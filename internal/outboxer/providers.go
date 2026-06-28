package outboxer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
	outboxpubsub "github.com/fvdsn/outboxer/internal/outboxer/pubsub"
	outboxsqs "github.com/fvdsn/outboxer/internal/outboxer/sqs"
)

const awsWebIdentityProviderGoogle = outboxsqs.WebIdentityProviderGoogle

const (
	eventTargetPubSub = outboxpubsub.Target
	eventTargetSQS    = outboxsqs.Target
)

type providerRoute struct {
	target             string
	defaultDestination string
	ownedDestinations  []string
}

type providerSpec struct {
	target  string
	enabled func(appConfig) bool
	route   func(appConfig) providerRoute
	build   func(context.Context, appConfig) (provider.Sender, func(), error)
}

var providerSpecs = []providerSpec{
	{
		target: eventTargetPubSub,
		enabled: func(cfg appConfig) bool {
			return cfg.PubSubEnabled
		},
		route: func(cfg appConfig) providerRoute {
			return providerRoute{
				target:             eventTargetPubSub,
				defaultDestination: cfg.DefaultPubSubTopic,
				ownedDestinations:  append([]string(nil), cfg.PubSubDestinations...),
			}
		},
		build: buildPubSubSender,
	},
	{
		target: eventTargetSQS,
		enabled: func(cfg appConfig) bool {
			return cfg.SQSEnabled
		},
		route: func(cfg appConfig) providerRoute {
			return providerRoute{
				target:             eventTargetSQS,
				defaultDestination: cfg.DefaultSQSQueueURL,
				ownedDestinations:  append([]string(nil), cfg.SQSDestinations...),
			}
		},
		build: buildSQSSender,
	},
}

func configuredProviderRoutes(cfg appConfig) []providerRoute {
	routes := []providerRoute{}
	for _, spec := range providerSpecs {
		if spec.enabled(cfg) {
			routes = append(routes, spec.route(cfg))
		}
	}
	return routes
}

func buildProviderSenders(ctx context.Context, cfg appConfig) (map[string]provider.Sender, func(), error) {
	senders := map[string]provider.Sender{}
	cleanups := []func(){}
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	for _, spec := range providerSpecs {
		if !spec.enabled(cfg) {
			continue
		}
		sender, closeProvider, err := spec.build(ctx, cfg)
		if err != nil {
			cleanup()
			return nil, func() {}, fmt.Errorf("create %s provider: %w", spec.target, err)
		}
		senders[spec.target] = sender
		cleanups = append(cleanups, closeProvider)
	}
	return senders, cleanup, nil
}

func buildPubSubSender(ctx context.Context, cfg appConfig) (provider.Sender, func(), error) {
	providerConfig := pubsubConfig(cfg)
	client, err := outboxpubsub.NewClient(ctx, providerConfig)
	if err != nil {
		return nil, func() {}, err
	}
	publisher := outboxpubsub.NewCloudPublisher(client, providerConfig)
	cleanup := func() {
		if err := publisher.Close(); err != nil {
			slog.Error("Failed to close Pub/Sub publisher", "error", err.Error())
		}
		if err := client.Close(); err != nil {
			slog.Error("Failed to close Pub/Sub client", "error", err.Error())
		}
	}
	return outboxpubsub.NewSender(providerConfig, publisher), cleanup, nil
}

func buildSQSSender(ctx context.Context, cfg appConfig) (provider.Sender, func(), error) {
	providerConfig := sqsConfig(cfg)
	client, err := outboxsqs.NewClient(ctx, providerConfig)
	if err != nil {
		return nil, func() {}, err
	}
	return outboxsqs.NewSender(providerConfig, outboxsqs.NewPublisher(client)), func() {}, nil
}

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

func providerEvents(events []event) []provider.Event {
	converted := make([]provider.Event, len(events))
	for i, evt := range events {
		converted[i] = providerEvent(evt)
	}
	return converted
}
