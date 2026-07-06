package pubsub

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
)

// CloudPublisher adapts the Google Cloud client to the provider publisher contract.
type CloudPublisher struct {
	mu           sync.Mutex
	publishers   map[string]pubsubTopicPublisher
	newPublisher func(topic string) pubsubTopicPublisher
}

type cloudPubSubPublishResult struct {
	result *pubsub.PublishResult
}

type pubsubTopicPublisher interface {
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
	Flush()
	ResumePublish(orderingKey string)
	Stop()
}

// NewClient creates a configured Google Cloud Pub/Sub client.
func NewClient(ctx context.Context, cfg Config) (*pubsub.Client, error) {
	options := []option.ClientOption{}
	if cfg.APIEndpoint != "" {
		options = append(options, option.WithEndpoint(cfg.APIEndpoint))
	}
	return pubsub.NewClient(ctx, cfg.ProjectID, options...)
}

// NewCloudPublisher creates a topic-caching Pub/Sub publisher.
func NewCloudPublisher(client *pubsub.Client, cfg Config) *CloudPublisher {
	p := &CloudPublisher{
		publishers: map[string]pubsubTopicPublisher{},
	}
	p.newPublisher = func(topic string) pubsubTopicPublisher {
		publisher := client.Publisher(topic)
		publisher.EnableMessageOrdering = true
		publisher.PublishSettings.Timeout = cfg.PublishTimeout
		return publisher
	}
	return p
}

// Publish starts publishing one message.
func (p *CloudPublisher) Publish(ctx context.Context, message Message) PublishResult {
	pubsubMsg := &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	}
	if message.OrderingKey != "" {
		pubsubMsg.OrderingKey = message.OrderingKey
	}
	return cloudPubSubPublishResult{result: p.publisher(message.Topic).Publish(ctx, pubsubMsg)}
}

// Flush sends all buffered messages for a topic.
func (p *CloudPublisher) Flush(topic string) {
	p.publisher(topic).Flush()
}

// ResumePublish resumes an ordering key after a publish failure.
func (p *CloudPublisher) ResumePublish(topic string, orderingKey string) {
	if orderingKey == "" {
		return
	}
	p.publisher(topic).ResumePublish(orderingKey)
}

// Close stops every cached topic publisher.
func (p *CloudPublisher) Close() error {
	p.mu.Lock()
	publishers := make([]pubsubTopicPublisher, 0, len(p.publishers))
	for _, publisher := range p.publishers {
		publishers = append(publishers, publisher)
	}
	p.mu.Unlock()

	for _, publisher := range publishers {
		publisher.Stop()
	}
	return nil
}

func (p *CloudPublisher) publisher(topic string) pubsubTopicPublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	publisher, ok := p.publishers[topic]
	if ok {
		return publisher
	}

	publisher = p.newPublisher(topic)
	p.publishers[topic] = publisher
	return publisher
}

func (r cloudPubSubPublishResult) Get(ctx context.Context) (string, error) {
	return r.result.Get(ctx)
}
