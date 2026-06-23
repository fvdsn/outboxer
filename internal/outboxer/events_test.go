package outboxer

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestEventBackendOptionsParseSelectedBackend(t *testing.T) {
	cfg := testConfig()
	evt := event{columns: map[string]any{
		"options": []byte(`{"pubsub":{"orderingKey":"key-a","attributes":{"trace":"abc"}},"sqs":{"messageGroupId":"group-a"}}`),
	}}

	pubsub, err := eventPubSubOptions(evt, cfg)
	if err != nil {
		t.Fatalf("eventPubSubOptions returned error: %v", err)
	}
	orderingKey, err := pubsub.stringValue("orderingKey")
	if err != nil {
		t.Fatalf("orderingKey returned error: %v", err)
	}
	if orderingKey != "key-a" {
		t.Fatalf("expected Pub/Sub ordering key, got %q", orderingKey)
	}
	attributes, err := pubsub.attributesValue("attributes")
	if err != nil {
		t.Fatalf("attributes returned error: %v", err)
	}
	if !reflect.DeepEqual(attributes, map[string]any{"trace": "abc"}) {
		t.Fatalf("unexpected attributes: %#v", attributes)
	}

	sqs, err := eventSQSOptions(evt, cfg)
	if err != nil {
		t.Fatalf("eventSQSOptions returned error: %v", err)
	}
	messageGroupID, err := sqs.stringValue("messageGroupId")
	if err != nil {
		t.Fatalf("messageGroupId returned error: %v", err)
	}
	if messageGroupID != "group-a" {
		t.Fatalf("expected SQS message group id, got %q", messageGroupID)
	}
}

func TestEventBackendOptionsTreatsMissingOrDisabledOptionsAsEmpty(t *testing.T) {
	cfg := testConfig()

	for _, evt := range []event{
		{columns: map[string]any{}},
		{columns: map[string]any{"options": nil}},
		{columns: map[string]any{"options": []byte(`null`)}},
	} {
		options, err := eventPubSubOptions(evt, cfg)
		if err != nil {
			t.Fatalf("eventPubSubOptions returned error: %v", err)
		}
		if got, err := options.stringValue("orderingKey"); err != nil || got != "" {
			t.Fatalf("expected empty ordering key, got %q error %v", got, err)
		}
	}

	cfg.EventOptions = ""
	options, err := eventSQSOptions(event{columns: map[string]any{"options": []byte(`{"sqs":{"messageGroupId":"ignored"}}`)}}, cfg)
	if err != nil {
		t.Fatalf("eventSQSOptions returned error: %v", err)
	}
	if got, err := options.stringValue("messageGroupId"); err != nil || got != "" {
		t.Fatalf("expected disabled options to be empty, got %q error %v", got, err)
	}
}

func TestEventBackendOptionsRejectsMalformedOptions(t *testing.T) {
	cfg := testConfig()
	tests := []struct {
		name  string
		evt   event
		check func(backendOptions) error
	}{
		{name: "root", evt: event{columns: map[string]any{"options": []byte(`[]`)}}},
		{name: "section", evt: event{columns: map[string]any{"options": []byte(`{"pubsub":[]`)}}},
		{
			name: "ordering key",
			evt:  event{columns: map[string]any{"options": []byte(`{"pubsub":{"orderingKey":42}}`)}},
			check: func(options backendOptions) error {
				_, err := options.stringValue("orderingKey")
				return err
			},
		},
		{
			name: "attributes",
			evt:  event{columns: map[string]any{"options": []byte(`{"pubsub":{"attributes":[]}}`)}},
			check: func(options backendOptions) error {
				_, err := options.attributesValue("attributes")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := eventPubSubOptions(tt.evt, cfg)
			if err == nil && tt.check != nil {
				err = tt.check(options)
			}
			if !errors.Is(err, errMalformedOptions) {
				t.Fatalf("expected malformed options error, got %v", err)
			}
		})
	}
}

func TestEventBackendOptionsIgnoresUnknownKeys(t *testing.T) {
	cfg := testConfig()
	options, err := eventPubSubOptions(event{columns: map[string]any{"options": []byte(`{"pubsub":{"unknown":42}}`)}}, cfg)
	if err != nil {
		t.Fatalf("eventPubSubOptions returned error: %v", err)
	}
	if got, err := options.stringValue("orderingKey"); err != nil || got != "" {
		t.Fatalf("expected unknown keys to be ignored, got %q error %v", got, err)
	}
}

func TestLegacyMetadataColumnsAreIgnored(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	evt := event{columns: map[string]any{
		"id":           "event-1",
		"destination":  "topic-1",
		"payload":      "payload",
		"ordering_key": "legacy-key",
		"attributes":   []byte(`{"legacy":"ignored"}`),
	}}

	if err := a.sendPubsubEvent(context.Background(), evt, func(any) {}); err != nil {
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
	a := &app{cfg: cfg, pubsub: pubsub}
	events := []event{{columns: map[string]any{
		"id":          "event-1",
		"destination": "topic-1",
		"payload":     "payload",
		"options":     []byte(`{"pubsub":{"attributes":[]}}`),
	}}}

	output, err := a.collectSenderOutput(context.Background(), events, func(callbacks senderCallbacks) error {
		return a.sendPubsubEventsWithCallbacks(context.Background(), events, callbacks)
	})
	if err != nil {
		t.Fatalf("collectSenderOutput returned error: %v", err)
	}
	if len(output.poison) != 1 {
		t.Fatalf("expected malformed options to report one poison event, got %#v", output)
	}
}
