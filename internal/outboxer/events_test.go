package outboxer

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

func TestEventOptionsParsesSelectedSection(t *testing.T) {
	raw := []byte(`{"pubsub":{"orderingKey":"key-a","attributes":{"trace":"abc"}},"sqs":{"messageGroupId":"group-a"}}`)

	pubsub, err := eventOptions(raw, eventTargetPubSub)
	if err != nil {
		t.Fatalf("eventOptions(pubsub) returned error: %v", err)
	}
	orderingKey, err := pubsub.String("orderingKey")
	if err != nil {
		t.Fatalf("orderingKey returned error: %v", err)
	}
	if orderingKey != "key-a" {
		t.Fatalf("expected Pub/Sub ordering key, got %q", orderingKey)
	}
	attributes, err := pubsub.Object("attributes")
	if err != nil {
		t.Fatalf("attributes returned error: %v", err)
	}
	if !reflect.DeepEqual(attributes, map[string]any{"trace": "abc"}) {
		t.Fatalf("unexpected attributes: %#v", attributes)
	}

	sqs, err := eventOptions(raw, eventTargetSQS)
	if err != nil {
		t.Fatalf("eventOptions(sqs) returned error: %v", err)
	}
	messageGroupID, err := sqs.String("messageGroupId")
	if err != nil {
		t.Fatalf("messageGroupId returned error: %v", err)
	}
	if messageGroupID != "group-a" {
		t.Fatalf("expected SQS message group id, got %q", messageGroupID)
	}
}

func TestEventOptionsTreatsMissingAsEmpty(t *testing.T) {
	for _, raw := range []any{nil, []byte(`null`), []byte(`{"sqs":{"messageGroupId":"other"}}`)} {
		options, err := eventOptions(raw, eventTargetPubSub)
		if err != nil {
			t.Fatalf("eventOptions returned error: %v", err)
		}
		if got, err := options.String("orderingKey"); err != nil || got != "" {
			t.Fatalf("expected empty ordering key, got %q error %v", got, err)
		}
	}
}

func TestEventOptionsRejectsMalformed(t *testing.T) {
	tests := []struct {
		name  string
		raw   any
		check func(provider.Options) error
	}{
		{name: "root", raw: []byte(`[]`)},
		{name: "section", raw: []byte(`{"pubsub":[]`)},
		{name: "section type", raw: []byte(`{"pubsub":[]}`)},
		{
			name: "ordering key",
			raw:  []byte(`{"pubsub":{"orderingKey":42}}`),
			check: func(options provider.Options) error {
				_, err := options.String("orderingKey")
				return err
			},
		},
		{
			name: "attributes",
			raw:  []byte(`{"pubsub":{"attributes":[]}}`),
			check: func(options provider.Options) error {
				_, err := options.Object("attributes")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := eventOptions(tt.raw, eventTargetPubSub)
			if err == nil && tt.check != nil {
				err = tt.check(options)
			}
			if !errors.Is(err, provider.ErrMalformedOptions) {
				t.Fatalf("expected malformed options error, got %v", err)
			}
		})
	}
}

func TestEventOptionsIgnoresUnknownKeys(t *testing.T) {
	options, err := eventOptions([]byte(`{"pubsub":{"unknown":42}}`), eventTargetPubSub)
	if err != nil {
		t.Fatalf("eventOptions returned error: %v", err)
	}
	if got, err := options.String("orderingKey"); err != nil || got != "" {
		t.Fatalf("expected unknown keys to be ignored, got %q error %v", got, err)
	}
}

func TestLegacyMetadataColumnsAreIgnored(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg}
	setTestPubSubProvider(a, pubsub)
	evt := event{columns: map[string]any{
		"id":           "event-1",
		"destination":  "topic-1",
		"payload":      "payload",
		"ordering_key": "legacy-key",
		"attributes":   []byte(`{"legacy":"ignored"}`),
	}}

	if err := a.sendPubsubEventForTest(context.Background(), evt, func(any) {}); err != nil {
		t.Fatalf("sendPubsubEvent returned error: %v", err)
	}
	if len(pubsub.messages) != 1 {
		t.Fatalf("expected one message, got %#v", pubsub.messages)
	}
	message := pubsub.messages[0]
	if message.OrderingKey != "" {
		t.Fatalf("expected legacy ordering key to be ignored, got %q", message.OrderingKey)
	}
	if len(message.Attributes) != 0 {
		t.Fatalf("expected legacy attributes to be ignored, got %#v", message.Attributes)
	}
}

func TestMalformedOptionsAreReportedAsPoison(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg}
	setTestPubSubProvider(a, pubsub)
	events := []event{{
		columns: map[string]any{
			"id":          "event-1",
			"destination": "topic-1",
			"payload":     "payload",
			"options":     []byte(`{"pubsub":{"attributes":[]}}`),
		},
		route: eventRoute{target: eventTargetPubSub, destination: "topic-1"},
	}}

	output, err := a.collectProviderOutput(context.Background(), a.senders[eventTargetPubSub], events)
	if err != nil {
		t.Fatalf("collectSenderOutput returned error: %v", err)
	}
	if len(output.poison) != 1 {
		t.Fatalf("expected malformed options to report one poison event, got %#v", output)
	}
}

func TestEventTimestampParsesUTCInstants(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  time.Time
		ok    bool
	}{
		{"time", time.Date(2026, 6, 23, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60)), time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC), true},
		{"rfc3339 offset", "2026-06-23T12:00:00+02:00", time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC), true},
		{"timestamp without zone", "2026-06-23 12:00:00", time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC), true},
		{"timestamp without zone bytes", []byte("2026-06-23 12:00:00.123456"), time.Date(2026, 6, 23, 12, 0, 0, 123456000, time.UTC), true},
		{"empty", "", time.Time{}, false},
		{"bad", "not-a-time", time.Time{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := eventTimestamp(tt.value)
			if ok != tt.ok || !got.Equal(tt.want) {
				t.Fatalf("eventTimestamp(%#v) = %s, %t; want %s, %t", tt.value, got, ok, tt.want, tt.ok)
			}
		})
	}
}
