package harness

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// NewSQSClient connects to real SQS with the default credential chain.
func NewSQSClient(ctx context.Context, t *testing.T, region string) *awssqs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return awssqs.NewFromConfig(cfg)
}

// SQSSink implements MessageSink over one or more SQS queues (a standard and
// a FIFO queue, typically), polled together.
type SQSSink struct {
	client    *awssqs.Client
	queueURLs []string
}

// NewSQSSink wraps queues as a MessageSink.
func NewSQSSink(client *awssqs.Client, queueURLs ...string) *SQSSink {
	return &SQSSink{client: client, queueURLs: queueURLs}
}

func (s *SQSSink) receiveOnce(ctx context.Context, queueURL string, wait int32) ([]ReceivedMessage, error) {
	output, err := s.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     wait,
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
			sqstypes.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		return nil, err
	}
	messages := make([]ReceivedMessage, 0, len(output.Messages))
	for _, msg := range output.Messages {
		messages = append(messages, ReceivedMessage{
			Body:        aws.ToString(msg.Body),
			OrderingKey: msg.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)],
		})
		if _, err := s.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: msg.ReceiptHandle,
		}); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

// Receive polls every queue until want messages with the payload prefix
// arrived or the timeout passed. Non-matching messages are deleted and
// dropped.
func (s *SQSSink) Receive(ctx context.Context, t *testing.T, prefix string, want int, timeout time.Duration) []ReceivedMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	messages := []ReceivedMessage{}
	for len(messages) < want {
		if time.Now().After(deadline) {
			t.Fatalf("received %d of %d %q messages from SQS within %s", len(messages), want, prefix, timeout)
		}
		for _, queueURL := range s.queueURLs {
			batch, err := s.receiveOnce(ctx, queueURL, 1)
			if err != nil {
				t.Fatalf("receive from %s: %v", queueURL, err)
			}
			for _, msg := range batch {
				if strings.HasPrefix(msg.Body, prefix) {
					messages = append(messages, msg)
				}
			}
		}
	}
	return messages
}

// Stream delivers every message on a channel until stop is called.
func (s *SQSSink) Stream(ctx context.Context, t *testing.T) (<-chan ReceivedMessage, func()) {
	t.Helper()
	streamCtx, cancel := context.WithCancel(ctx)
	out := make(chan ReceivedMessage, 1000)
	var wg sync.WaitGroup
	for _, queueURL := range s.queueURLs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for streamCtx.Err() == nil {
				batch, err := s.receiveOnce(streamCtx, queueURL, 2)
				if err != nil {
					if streamCtx.Err() != nil {
						return
					}
					time.Sleep(time.Second)
					continue
				}
				for _, msg := range batch {
					select {
					case out <- msg:
					case <-streamCtx.Done():
						return
					}
				}
			}
		}()
	}
	return out, func() {
		cancel()
		wg.Wait()
	}
}

// Purge drops everything in every queue — best-effort hygiene after bulk
// scenarios (SQS purges complete asynchronously within a minute).
func (s *SQSSink) Purge(ctx context.Context, t *testing.T) {
	t.Helper()
	for _, queueURL := range s.queueURLs {
		if _, err := s.client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
			t.Logf("purge %s (best effort): %v", queueURL, err)
		}
	}
}
