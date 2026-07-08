package harness

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReceivedMessage is one message pulled from the real backend.
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

// PubSubSink implements MessageSink over one Pub/Sub subscription.
type PubSubSink struct {
	client       *pubsub.Client
	projectID    string
	subscription string
}

// NewPubSubSink wraps a subscription as a MessageSink.
func NewPubSubSink(client *pubsub.Client, projectID string, subscription string) *PubSubSink {
	return &PubSubSink{client: client, projectID: projectID, subscription: subscription}
}

// Receive pulls until want messages with the payload prefix arrived or the
// timeout passed. Non-matching messages (stale runs, settle canaries) are
// acked and dropped.
func (s *PubSubSink) Receive(ctx context.Context, t *testing.T, prefix string, want int, timeout time.Duration) []ReceivedMessage {
	t.Helper()
	receiveCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	subscriber := s.client.Subscriber(s.subscription)
	subscriber.ReceiveSettings.MaxOutstandingMessages = 1000

	var mu sync.Mutex
	messages := []ReceivedMessage{}
	err := subscriber.Receive(receiveCtx, func(_ context.Context, msg *pubsub.Message) {
		msg.Ack()
		if !strings.HasPrefix(string(msg.Data), prefix) {
			return
		}
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
		t.Fatalf("receive from %s: %v", s.subscription, err)
	}
	if len(messages) < want {
		t.Fatalf("received %d of %d %q messages from %s within %s", len(messages), want, prefix, s.subscription, timeout)
	}
	return messages
}

// Stream delivers every message on a channel until stop is called.
func (s *PubSubSink) Stream(ctx context.Context, t *testing.T) (<-chan ReceivedMessage, func()) {
	t.Helper()
	streamCtx, cancel := context.WithCancel(ctx)
	out := make(chan ReceivedMessage, 1000)
	subscriber := s.client.Subscriber(s.subscription)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = subscriber.Receive(streamCtx, func(_ context.Context, msg *pubsub.Message) {
			msg.Ack()
			select {
			case out <- ReceivedMessage{Body: string(msg.Data), OrderingKey: msg.OrderingKey}:
			case <-streamCtx.Done():
			}
		})
	}()
	// Give the streaming pull a moment to open, so receiver startup does not
	// count against the first measurement.
	time.Sleep(2 * time.Second)
	return out, func() {
		cancel()
		<-done
	}
}

// Purge acknowledges everything currently in the subscription by seeking to
// now — best-effort hygiene after bulk scenarios.
func (s *PubSubSink) Purge(ctx context.Context, t *testing.T) {
	t.Helper()
	_, err := s.client.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: "projects/" + s.projectID + "/subscriptions/" + s.subscription,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now())},
	})
	if err != nil {
		t.Logf("purge subscription %s (best effort): %v", s.subscription, err)
	}
}
