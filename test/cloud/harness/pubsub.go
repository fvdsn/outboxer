package harness

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReceivedMessage is one message pulled from the real subscription.
type ReceivedMessage struct {
	Body        string
	OrderingKey string
}

// NewPubSubClient connects to real Pub/Sub with ADC.
func NewPubSubClient(ctx context.Context, t *testing.T, projectID string) *pubsub.Client {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("pubsub client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// ReceiveMessages pulls until want messages arrived or the timeout passed.
func ReceiveMessages(ctx context.Context, t *testing.T, client *pubsub.Client, subscription string, want int, timeout time.Duration) []ReceivedMessage {
	t.Helper()
	receiveCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	subscriber := client.Subscriber(subscription)
	subscriber.ReceiveSettings.MaxOutstandingMessages = 1000

	var mu sync.Mutex
	messages := []ReceivedMessage{}
	err := subscriber.Receive(receiveCtx, func(_ context.Context, msg *pubsub.Message) {
		msg.Ack()
		mu.Lock()
		messages = append(messages, ReceivedMessage{
			Body:        string(msg.Data),
			OrderingKey: msg.OrderingKey,
		})
		if len(messages) >= want {
			cancel()
		}
		mu.Unlock()
	})
	if err != nil && receiveCtx.Err() == nil {
		t.Fatalf("receive from %s: %v", subscription, err)
	}
	if len(messages) < want {
		t.Fatalf("received %d of %d messages from %s within %s", len(messages), want, subscription, timeout)
	}
	return messages
}

// PurgeSubscription acknowledges everything currently in the subscription by
// seeking it to now — used after perf runs, where pulling hundreds of
// thousands of messages to a laptop would add nothing.
func PurgeSubscription(ctx context.Context, t *testing.T, client *pubsub.Client, projectID string, subscription string) {
	t.Helper()
	_, err := client.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: "projects/" + projectID + "/subscriptions/" + subscription,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now())},
	})
	if err != nil {
		t.Fatalf("purge subscription %s: %v", subscription, err)
	}
}
