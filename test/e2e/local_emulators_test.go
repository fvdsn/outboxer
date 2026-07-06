//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	e2eProjectID      = "outboxer-test"
	e2ePubSubEndpoint = "localhost:8085"
	e2eSQSEndpoint    = "http://localhost:9324"
	e2ePGDSN          = "postgres://outboxer:outboxer@localhost:54329/outboxer?sslmode=disable"
	e2eAWSRegion      = "us-east-1"
)

type pubsubReceivedMessage struct {
	body        string
	orderingKey string
	attributes  map[string]string
}

type sqsReceivedMessage struct {
	body       string
	groupID    string
	attributes map[string]string
}

func TestLocalEmulatorE2EMixedBackendsDestinationsAndOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	createEventsTable(t, ctx, db, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	})

	t.Setenv("PUBSUB_EMULATOR_HOST", e2ePubSubEndpoint)
	pubsubClient := newPubSubClient(t, ctx)
	defer pubsubClient.Close()

	pubsubUnorderedA := "ps_unordered_a_" + runID
	pubsubUnorderedB := "ps_unordered_b_" + runID
	pubsubOrdered := "ps_ordered_" + runID
	pubsubTopics := []string{pubsubUnorderedA, pubsubUnorderedB, pubsubOrdered}
	pubsubSubscriptions := map[string]string{}
	for _, topic := range pubsubTopics {
		createPubSubTopicAndSubscription(t, ctx, pubsubClient, topic, topic+"_sub")
		pubsubSubscriptions[topic] = topic + "_sub"
	}

	sqsClient := newSQSClient(t, ctx)
	standardA := createSQSQueue(t, ctx, sqsClient, "standard-a-"+runID, false)
	standardB := createSQSQueue(t, ctx, sqsClient, "standard-b-"+runID, false)
	fifo := createSQSQueue(t, ctx, sqsClient, "fifo-"+runID+".fifo", true)

	events := []eventRow{}
	for i := 0; i < 12; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("pubsub-unordered-a-%02d", i),
			target:      "pubsub",
			destination: pubsubUnorderedA,
			payload:     fmt.Sprintf("pubsub-unordered-a-%02d", i),
			attributes:  `{"kind":"pubsub-unordered","topic":"a"}`,
		})
	}
	for i := 0; i < 8; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("pubsub-unordered-b-%02d", i),
			target:      "pubsub",
			destination: pubsubUnorderedB,
			payload:     fmt.Sprintf("pubsub-unordered-b-%02d", i),
			attributes:  `{"kind":"pubsub-unordered","topic":"b"}`,
		})
	}
	for _, key := range []string{"alpha", "beta"} {
		for i := 0; i < 6; i++ {
			events = append(events, eventRow{
				id:          fmt.Sprintf("pubsub-ordered-%s-%02d", key, i),
				target:      "pubsub",
				destination: pubsubOrdered,
				orderingKey: key,
				payload:     fmt.Sprintf("pubsub-ordered-%s-%02d", key, i),
				attributes:  fmt.Sprintf(`{"kind":"pubsub-ordered","key":"%s"}`, key),
			})
		}
	}
	for i := 0; i < 15; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("sqs-standard-a-%02d", i),
			target:      "sqs",
			destination: standardA,
			payload:     fmt.Sprintf("sqs-standard-a-%02d", i),
			attributes:  `{"kind":"sqs-standard","queue":"a"}`,
		})
	}
	for i := 0; i < 11; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("sqs-standard-b-%02d", i),
			target:      "sqs",
			destination: standardB,
			payload:     fmt.Sprintf("sqs-standard-b-%02d", i),
			attributes:  `{"kind":"sqs-standard","queue":"b"}`,
		})
	}
	for _, group := range []string{"red", "blue"} {
		for i := 0; i < 6; i++ {
			events = append(events, eventRow{
				id:          fmt.Sprintf("sqs-fifo-%s-%02d", group, i),
				target:      "sqs",
				destination: fifo,
				orderingKey: group,
				payload:     fmt.Sprintf("sqs-fifo-%s-%02d", group, i),
				attributes:  fmt.Sprintf(`{"kind":"sqs-fifo","group":"%s"}`, group),
			})
		}
	}
	insertEvents(t, ctx, db, table, events)

	process := startOutboxer(t, ctx, binary, table)

	pubsubResults := map[string][]pubsubReceivedMessage{}
	for topic, subscription := range pubsubSubscriptions {
		want := map[string]int{
			pubsubUnorderedA: 12,
			pubsubUnorderedB: 8,
			pubsubOrdered:    12,
		}[topic]
		pubsubResults[topic] = receivePubSubMessages(t, ctx, pubsubClient, subscription, want)
	}

	sqsStandardA := receiveSQSMessages(t, ctx, sqsClient, standardA, 15)
	sqsStandardB := receiveSQSMessages(t, ctx, sqsClient, standardB, 11)
	sqsFIFO := receiveSQSMessages(t, ctx, sqsClient, fifo, 12)

	waitForEmptyTable(t, ctx, db, table)
	stopOutboxer(t, process)

	assertBodies(t, pubsubResults[pubsubUnorderedA], "pubsub-unordered-a-", 12)
	assertBodies(t, pubsubResults[pubsubUnorderedB], "pubsub-unordered-b-", 8)
	assertPubSubOrdering(t, pubsubResults[pubsubOrdered], "alpha", 6)
	assertPubSubOrdering(t, pubsubResults[pubsubOrdered], "beta", 6)
	assertBodies(t, sqsStandardA, "sqs-standard-a-", 15)
	assertBodies(t, sqsStandardB, "sqs-standard-b-", 11)
	assertSQSOrdering(t, sqsFIFO, "red", 6)
	assertSQSOrdering(t, sqsFIFO, "blue", 6)
}

func TestLocalEmulatorE2ETwoOutboxersPreserveOrderedPubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	createEventsTable(t, ctx, db, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	})

	t.Setenv("PUBSUB_EMULATOR_HOST", e2ePubSubEndpoint)
	pubsubClient := newPubSubClient(t, ctx)
	defer pubsubClient.Close()

	topic := "ps_two_outboxers_ordered_" + runID
	subscription := topic + "_sub"
	createPubSubTopicAndSubscription(t, ctx, pubsubClient, topic, subscription)

	overrides := map[string]string{
		"PUBSUB_ENABLED":            "true",
		"SQS_ENABLED":               "false",
		"DEFAULT_PUBSUB_TOPIC":      topic,
		"COLLECT_BATCH_TARGET":      "40",
		"SQS_SEND_CONCURRENCY":      "1",
		"POLL_INTERVAL_MS":          "10",
		"PUBLISH_TIMEOUT_MS":        "5000",
		"PUBLISH_RESULT_GRACE_MS":   "500",
		"WATCHDOG_INTERVAL_MS":      "60000",
		"EVENT_TARGET":              "target",
		"EVENT_DESTINATION":         "destination",
		"EVENT_OPTIONS":             "options",
		"EVENT_PAYLOAD":             "payload",
		"EVENT_ID":                  "id",
		"EVENT_TIMESTAMP":           "timestamp",
		"DEFAULT_SQS_QUEUE_URL":     "disabled",
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	}
	first := startOutboxer(t, ctx, binary, table, overrides)
	second := startOutboxer(t, ctx, binary, table, overrides)

	const count = 50
	const key = "shared"
	events := make([]eventRow, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("pubsub-ordered-%s-%02d", key, i),
			target:      "pubsub",
			destination: topic,
			orderingKey: key,
			payload:     fmt.Sprintf("pubsub-ordered-%s-%02d", key, i),
			attributes:  fmt.Sprintf(`{"kind":"pubsub-ordered","key":"%s"}`, key),
		})
	}
	insertEvents(t, ctx, db, table, events)

	messages := receivePubSubMessages(t, ctx, pubsubClient, subscription, count)

	waitForEmptyTable(t, ctx, db, table)
	stopOutboxer(t, first)
	stopOutboxer(t, second)

	assertPubSubOrdering(t, messages, key, count)
}

func TestLocalEmulatorE2ETwoOutboxersSplitByTargetOnSameTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	createEventsTable(t, ctx, db, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	})

	t.Setenv("PUBSUB_EMULATOR_HOST", e2ePubSubEndpoint)
	pubsubClient := newPubSubClient(t, ctx)
	defer pubsubClient.Close()

	topic := "ps_split_target_" + runID
	subscription := topic + "_sub"
	createPubSubTopicAndSubscription(t, ctx, pubsubClient, topic, subscription)

	sqsClient := newSQSClient(t, ctx)
	queue := createSQSQueue(t, ctx, sqsClient, "split-target-"+runID, false)

	events := []eventRow{}
	for i := 0; i < 20; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("pubsub-split-%02d", i),
			target:      "pubsub",
			destination: topic,
			payload:     fmt.Sprintf("pubsub-split-%02d", i),
			attributes:  `{"kind":"pubsub-split"}`,
		})
		events = append(events, eventRow{
			id:          fmt.Sprintf("sqs-split-%02d", i),
			target:      "sqs",
			destination: queue,
			payload:     fmt.Sprintf("sqs-split-%02d", i),
			attributes:  `{"kind":"sqs-split"}`,
		})
	}
	insertEvents(t, ctx, db, table, events)

	commonOverrides := map[string]string{
		"COLLECT_BATCH_TARGET":      "5",
		"POLL_INTERVAL_MS":          "10",
		"ERROR_COOLDOWN_MS":         "50",
		"PUBLISH_TIMEOUT_MS":        "5000",
		"PUBLISH_RESULT_GRACE_MS":   "500",
		"WATCHDOG_INTERVAL_MS":      "60000",
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	}
	pubsubOverrides := copyStringMap(commonOverrides)
	pubsubOverrides["PUBSUB_ENABLED"] = "true"
	pubsubOverrides["SQS_ENABLED"] = "false"
	pubsubOverrides["DEFAULT_SQS_QUEUE_URL"] = "disabled"
	sqsOverrides := copyStringMap(commonOverrides)
	sqsOverrides["PUBSUB_ENABLED"] = "false"
	sqsOverrides["SQS_ENABLED"] = "true"
	sqsOverrides["DEFAULT_PUBSUB_TOPIC"] = "disabled"

	pubsubProcess := startOutboxer(t, ctx, binary, table, pubsubOverrides)
	sqsProcess := startOutboxer(t, ctx, binary, table, sqsOverrides)

	pubsubMessages := receivePubSubMessages(t, ctx, pubsubClient, subscription, 20)
	sqsMessages := receiveSQSMessages(t, ctx, sqsClient, queue, 20)

	waitForEmptyTable(t, ctx, db, table)
	stopOutboxer(t, pubsubProcess)
	stopOutboxer(t, sqsProcess)

	assertBodies(t, pubsubMessages, "pubsub-split-", 20)
	assertBodies(t, sqsMessages, "sqs-split-", 20)
}

func TestLocalEmulatorE2ETwoOutboxersSplitByPubSubDestination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	createEventsTable(t, ctx, db, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	})

	t.Setenv("PUBSUB_EMULATOR_HOST", e2ePubSubEndpoint)
	pubsubClient := newPubSubClient(t, ctx)
	defer pubsubClient.Close()

	topicA := "ps_split_destination_a_" + runID
	topicB := "ps_split_destination_b_" + runID
	subscriptionA := topicA + "_sub"
	subscriptionB := topicB + "_sub"
	createPubSubTopicAndSubscription(t, ctx, pubsubClient, topicA, subscriptionA)
	createPubSubTopicAndSubscription(t, ctx, pubsubClient, topicB, subscriptionB)

	events := []eventRow{}
	for i := 0; i < 25; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("pubsub-destination-a-%02d", i),
			target:      "pubsub",
			destination: topicA,
			payload:     fmt.Sprintf("pubsub-destination-a-%02d", i),
			attributes:  `{"kind":"pubsub-destination","topic":"a"}`,
		})
		events = append(events, eventRow{
			id:          fmt.Sprintf("pubsub-destination-b-%02d", i),
			target:      "pubsub",
			destination: topicB,
			payload:     fmt.Sprintf("pubsub-destination-b-%02d", i),
			attributes:  `{"kind":"pubsub-destination","topic":"b"}`,
		})
	}
	insertEvents(t, ctx, db, table, events)

	commonOverrides := map[string]string{
		"PUBSUB_ENABLED":            "true",
		"SQS_ENABLED":               "false",
		"DEFAULT_SQS_QUEUE_URL":     "disabled",
		"COLLECT_BATCH_TARGET":      "5",
		"POLL_INTERVAL_MS":          "10",
		"ERROR_COOLDOWN_MS":         "50",
		"PUBLISH_TIMEOUT_MS":        "5000",
		"PUBLISH_RESULT_GRACE_MS":   "500",
		"WATCHDOG_INTERVAL_MS":      "60000",
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	}
	overridesA := copyStringMap(commonOverrides)
	overridesA["PUBSUB_DESTINATIONS"] = topicA
	overridesB := copyStringMap(commonOverrides)
	overridesB["PUBSUB_DESTINATIONS"] = topicB

	processA := startOutboxer(t, ctx, binary, table, overridesA)
	processB := startOutboxer(t, ctx, binary, table, overridesB)

	messagesA := receivePubSubMessages(t, ctx, pubsubClient, subscriptionA, 25)
	messagesB := receivePubSubMessages(t, ctx, pubsubClient, subscriptionB, 25)

	waitForEmptyTable(t, ctx, db, table)
	stopOutboxer(t, processA)
	stopOutboxer(t, processB)

	assertBodies(t, messagesA, "pubsub-destination-a-", 25)
	assertBodies(t, messagesB, "pubsub-destination-b-", 25)
}

func TestLocalEmulatorE2ETwoOutboxersSplitBySQSDestination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	createEventsTable(t, ctx, db, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	})

	sqsClient := newSQSClient(t, ctx)
	queueA := createSQSQueue(t, ctx, sqsClient, "split-destination-a-"+runID, false)
	queueB := createSQSQueue(t, ctx, sqsClient, "split-destination-b-"+runID, false)

	events := []eventRow{}
	for i := 0; i < 25; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("sqs-destination-a-%02d", i),
			target:      "sqs",
			destination: queueA,
			payload:     fmt.Sprintf("sqs-destination-a-%02d", i),
			attributes:  `{"kind":"sqs-destination","queue":"a"}`,
		})
		events = append(events, eventRow{
			id:          fmt.Sprintf("sqs-destination-b-%02d", i),
			target:      "sqs",
			destination: queueB,
			payload:     fmt.Sprintf("sqs-destination-b-%02d", i),
			attributes:  `{"kind":"sqs-destination","queue":"b"}`,
		})
	}
	insertEvents(t, ctx, db, table, events)

	commonOverrides := map[string]string{
		"PUBSUB_ENABLED":            "false",
		"SQS_ENABLED":               "true",
		"DEFAULT_PUBSUB_TOPIC":      "disabled",
		"COLLECT_BATCH_TARGET":      "5",
		"POLL_INTERVAL_MS":          "10",
		"ERROR_COOLDOWN_MS":         "50",
		"PUBLISH_TIMEOUT_MS":        "5000",
		"PUBLISH_RESULT_GRACE_MS":   "500",
		"WATCHDOG_INTERVAL_MS":      "60000",
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	}
	overridesA := copyStringMap(commonOverrides)
	overridesA["SQS_DESTINATIONS"] = queueA
	overridesB := copyStringMap(commonOverrides)
	overridesB["SQS_DESTINATIONS"] = queueB

	processA := startOutboxer(t, ctx, binary, table, overridesA)
	processB := startOutboxer(t, ctx, binary, table, overridesB)

	messagesA := receiveSQSMessages(t, ctx, sqsClient, queueA, 25)
	messagesB := receiveSQSMessages(t, ctx, sqsClient, queueB, 25)

	waitForEmptyTable(t, ctx, db, table)
	stopOutboxer(t, processA)
	stopOutboxer(t, processB)

	assertBodies(t, messagesA, "sqs-destination-a-", 25)
	assertBodies(t, messagesB, "sqs-destination-b-", 25)
}

func TestLocalEmulatorE2ERouteBrokenDestinationDoesNotBlockHealthyRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	createEventsTable(t, ctx, db, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	})

	t.Setenv("PUBSUB_EMULATOR_HOST", e2ePubSubEndpoint)
	pubsubClient := newPubSubClient(t, ctx)
	defer pubsubClient.Close()

	topic := "ps_route_healthy_" + runID
	subscription := topic + "_sub"
	createPubSubTopicAndSubscription(t, ctx, pubsubClient, topic, subscription)

	sqsClient := newSQSClient(t, ctx)
	missingQueue := strings.TrimRight(getenv("OUTBOXER_E2E_SQS_ENDPOINT", e2eSQSEndpoint), "/") + "/000000000000/missing-" + runID
	if _, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(missingQueue)}); err == nil {
		t.Fatalf("missing test queue unexpectedly exists: %s", missingQueue)
	}

	events := []eventRow{}
	for i := 0; i < 12; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("000-sqs-broken-%02d", i),
			target:      "sqs",
			destination: missingQueue,
			payload:     fmt.Sprintf("sqs-broken-%02d", i),
			attributes:  `{"kind":"sqs-broken"}`,
		})
	}
	for i := 0; i < 3; i++ {
		events = append(events, eventRow{
			id:          fmt.Sprintf("900-pubsub-healthy-%02d", i),
			target:      "pubsub",
			destination: topic,
			payload:     fmt.Sprintf("pubsub-healthy-%02d", i),
			attributes:  `{"kind":"pubsub-healthy"}`,
		})
	}
	insertEvents(t, ctx, db, table, events)

	process := startOutboxer(t, ctx, binary, table, map[string]string{
		"COLLECT_BATCH_TARGET":      "5",
		"SQS_SEND_CONCURRENCY":      "1",
		"PUBLISH_TIMEOUT_MS":        "1000",
		"PUBLISH_RESULT_GRACE_MS":   "200",
		"ERROR_COOLDOWN_MS":         "50",
		"POLL_INTERVAL_MS":          "50",
		"WATCHDOG_INTERVAL_MS":      "60000",
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	})

	messages := receivePubSubMessages(t, ctx, pubsubClient, subscription, 3)
	waitForTableCount(t, ctx, db, table, 12)
	stopOutboxer(t, process)

	assertBodies(t, messages, "pubsub-healthy-", 3)
}

func TestLocalEmulatorE2EDeadLettersSQSPoisonEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	dlqTable := "outboxer_dlq_" + runID
	createEventsTable(t, ctx, db, table)
	createDLQTable(t, ctx, db, dlqTable)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(dlqTable)))
	})

	sqsClient := newSQSClient(t, ctx)
	queue := createSQSQueue(t, ctx, sqsClient, "dlq-"+runID, false)

	insertEvents(t, ctx, db, table, []eventRow{
		{
			id:          "poison-empty-body",
			target:      "",
			destination: "",
			payload:     "",
			attributes:  `null`,
		},
		{
			id:          "healthy",
			target:      "",
			destination: "",
			payload:     "healthy-00",
			attributes:  `{"kind":"healthy"}`,
		},
	})

	process := startOutboxer(t, ctx, binary, table, map[string]string{
		"PUBSUB_ENABLED":            "false",
		"SQS_ENABLED":               "true",
		"DEFAULT_PUBSUB_TOPIC":      "disabled",
		"DEFAULT_SQS_QUEUE_URL":     queue,
		"DLQ_TABLE":                 dlqTable,
		"COLLECT_BATCH_TARGET":      "10",
		"POLL_INTERVAL_MS":          "10",
		"ERROR_COOLDOWN_MS":         "50",
		"PUBLISH_TIMEOUT_MS":        "5000",
		"PUBLISH_RESULT_GRACE_MS":   "500",
		"WATCHDOG_INTERVAL_MS":      "60000",
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	})

	messages := receiveSQSMessages(t, ctx, sqsClient, queue, 1)
	waitForEmptyTable(t, ctx, db, table)
	deadLetters := readDLQEvents(t, ctx, db, dlqTable, 1)
	stopOutboxer(t, process)

	assertBodies(t, messages, "healthy-", 1)
	deadLetter := deadLetters[0]
	if deadLetter["target"] != "sqs" || deadLetter["destination"] != queue {
		t.Fatalf("unexpected resolved DLQ route: %#v", deadLetter)
	}
	if _, ok := deadLetter["reason"]; ok {
		t.Fatalf("DLQ payload must not include reason: %#v", deadLetter)
	}
	original, ok := deadLetter["original_event"].(map[string]any)
	if !ok {
		t.Fatalf("DLQ original_event has type %T: %#v", deadLetter["original_event"], deadLetter)
	}
	if original["id"] != "poison-empty-body" || original["payload"] != "" {
		t.Fatalf("unexpected original event in DLQ: %#v", original)
	}
}

// TestLocalEmulatorE2EDeadLettersBatchAcrossTargets processes one batch whose
// poison spans both providers: two SQS events with empty bodies and one
// Pub/Sub event with an invalid topic dead-letter together (a single batched
// insert) while a healthy event per target is delivered.
func TestLocalEmulatorE2EDeadLettersBatchAcrossTargets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	runID := strings.ReplaceAll(fmt.Sprintf("e2e-%d", time.Now().UnixNano()), "-", "_")

	db := openE2EDB(t, ctx)
	defer db.Close()
	table := "outboxer_" + runID
	dlqTable := "outboxer_dlq_" + runID
	createEventsTable(t, ctx, db, table)
	createDLQTable(t, ctx, db, dlqTable)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(dlqTable)))
	})

	t.Setenv("PUBSUB_EMULATOR_HOST", e2ePubSubEndpoint)
	pubsubClient := newPubSubClient(t, ctx)
	defer pubsubClient.Close()
	topic := "ps_dlq_batch_" + runID
	subscription := topic + "_sub"
	createPubSubTopicAndSubscription(t, ctx, pubsubClient, topic, subscription)

	sqsClient := newSQSClient(t, ctx)
	queue := createSQSQueue(t, ctx, sqsClient, "dlq-batch-"+runID, false)

	insertEvents(t, ctx, db, table, []eventRow{
		{id: "sqs-poison-00", target: "sqs", destination: queue, payload: "", attributes: `null`},
		{id: "sqs-poison-01", target: "sqs", destination: queue, payload: "", attributes: `null`},
		{id: "pubsub-poison-00", target: "pubsub", destination: "bad/topic", payload: "pubsub-poison", attributes: `null`},
		{id: "sqs-healthy-00", target: "sqs", destination: queue, payload: "sqs-healthy-00", attributes: `null`},
		{id: "pubsub-healthy-00", target: "pubsub", destination: topic, payload: "pubsub-healthy-00", attributes: `null`},
	})

	process := startOutboxer(t, ctx, binary, table, map[string]string{
		"DLQ_TABLE":                 dlqTable,
		"AWS_WEB_IDENTITY_PROVIDER": "disabled",
	})

	pubsubMessages := receivePubSubMessages(t, ctx, pubsubClient, subscription, 1)
	sqsMessages := receiveSQSMessages(t, ctx, sqsClient, queue, 1)
	waitForEmptyTable(t, ctx, db, table)
	deadLetters := readDLQEvents(t, ctx, db, dlqTable, 3)
	stopOutboxer(t, process)

	assertBodies(t, pubsubMessages, "pubsub-healthy-", 1)
	assertBodies(t, sqsMessages, "sqs-healthy-", 1)

	deadLettersByID := map[string]map[string]any{}
	for _, deadLetter := range deadLetters {
		original, ok := deadLetter["original_event"].(map[string]any)
		if !ok {
			t.Fatalf("DLQ original_event has type %T: %#v", deadLetter["original_event"], deadLetter)
		}
		id, _ := original["id"].(string)
		deadLettersByID[id] = deadLetter
	}
	wantRoutes := map[string]struct{ target, destination string }{
		"sqs-poison-00":    {"sqs", queue},
		"sqs-poison-01":    {"sqs", queue},
		"pubsub-poison-00": {"pubsub", "bad/topic"},
	}
	if len(deadLettersByID) != len(wantRoutes) {
		t.Fatalf("unexpected dead-lettered ids: %#v", deadLettersByID)
	}
	for id, route := range wantRoutes {
		deadLetter, ok := deadLettersByID[id]
		if !ok {
			t.Fatalf("expected %s to be dead-lettered, got %#v", id, deadLettersByID)
		}
		if deadLetter["target"] != route.target || deadLetter["destination"] != route.destination {
			t.Fatalf("unexpected resolved route for %s: %#v", id, deadLetter)
		}
		if errorText, ok := deadLetter["error"].(string); !ok || errorText == "" {
			t.Fatalf("expected a dead-letter error for %s: %#v", id, deadLetter)
		}
	}
}

type eventRow struct {
	id          string
	target      string
	destination string
	orderingKey string
	payload     string
	attributes  string
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func buildOutboxer(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "outboxer")
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/outboxer")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build outboxer: %v\n%s", err, output)
	}
	return binary
}

func openE2EDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := getenv("OUTBOXER_E2E_PG_DSN", e2ePGDSN)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open e2e database: %v", err)
	}
	waitUntil(t, ctx, "postgres", func(ctx context.Context) error {
		return db.PingContext(ctx)
	})
	return db
}

func createEventsTable(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id text PRIMARY KEY,
			timestamp timestamptz,
			payload text NOT NULL,
			target text,
			destination text,
			options jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create events table: %v", err)
	}
}

func createDLQTable(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			event jsonb NOT NULL
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create DLQ table: %v", err)
	}
}

func insertEvents(t *testing.T, ctx context.Context, db *sql.DB, table string, events []eventRow) {
	t.Helper()
	for _, evt := range events {
		options := eventOptionsJSON(evt)
		_, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, timestamp, payload, target, destination, options)
			VALUES ($1, now(), $2, $3, $4, $5::jsonb)
		`, ident(table)), evt.id, evt.payload, evt.target, evt.destination, options)
		if err != nil {
			t.Fatalf("insert event %s: %v", evt.id, err)
		}
	}
}

func eventOptionsJSON(evt eventRow) string {
	backend := evt.target
	if backend == "" {
		backend = eventTargetForDefaultOptions(evt)
	}
	section := map[string]any{}
	if evt.orderingKey != "" {
		if backend == "sqs" {
			section["messageGroupId"] = evt.orderingKey
		} else {
			section["orderingKey"] = evt.orderingKey
		}
	}
	if evt.attributes != "" && evt.attributes != "null" {
		var attributes map[string]any
		if err := json.Unmarshal([]byte(evt.attributes), &attributes); err == nil {
			if backend == "sqs" {
				attributes = nativeSQSAttributes(attributes)
			}
			section["attributes"] = attributes
		}
	}
	if len(section) == 0 || backend == "" {
		return "{}"
	}
	payload, err := json.Marshal(map[string]any{backend: section})
	if err != nil {
		return "{}"
	}
	return string(payload)
}

func nativeSQSAttributes(attributes map[string]any) map[string]any {
	native := make(map[string]any, len(attributes))
	for key, value := range attributes {
		if stringValue, ok := value.(string); ok {
			native[key] = map[string]any{"DataType": "String", "StringValue": stringValue}
		} else {
			native[key] = value
		}
	}
	return native
}

func eventTargetForDefaultOptions(evt eventRow) string {
	if strings.Contains(evt.id, "sqs") || strings.Contains(evt.destination, "sqs") || strings.Contains(evt.destination, "elasticmq") {
		return "sqs"
	}
	return "pubsub"
}

func readDLQEvents(t *testing.T, ctx context.Context, db *sql.DB, table string, want int) []map[string]any {
	t.Helper()
	var payloads []map[string]any
	waitUntil(t, ctx, fmt.Sprintf("DLQ table count %d", want), func(ctx context.Context) error {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT event::text FROM %s ORDER BY id", ident(table)))
		if err != nil {
			return err
		}
		defer rows.Close()

		payloads = nil
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				return err
			}
			payloads = append(payloads, payload)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(payloads) != want {
			return fmt.Errorf("found %d DLQ rows, want %d", len(payloads), want)
		}
		return nil
	})
	return append([]map[string]any(nil), payloads...)
}

func newPubSubClient(t *testing.T, ctx context.Context) *pubsub.Client {
	t.Helper()
	var client *pubsub.Client
	waitUntil(t, ctx, "pubsub emulator", func(ctx context.Context) error {
		var err error
		client, err = pubsub.NewClient(ctx, e2eProjectID)
		if err != nil {
			return err
		}
		return nil
	})
	return client
}

func createPubSubTopicAndSubscription(t *testing.T, ctx context.Context, client *pubsub.Client, topicID string, subscriptionID string) {
	t.Helper()
	topicName := fmt.Sprintf("projects/%s/topics/%s", e2eProjectID, topicID)
	subscriptionName := fmt.Sprintf("projects/%s/subscriptions/%s", e2eProjectID, subscriptionID)
	waitUntil(t, ctx, "pubsub topic "+topicID, func(ctx context.Context) error {
		_, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
		return err
	})
	waitUntil(t, ctx, "pubsub subscription "+subscriptionID, func(ctx context.Context) error {
		_, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:                  subscriptionName,
			Topic:                 topicName,
			AckDeadlineSeconds:    10,
			EnableMessageOrdering: true,
		})
		return err
	})
}

func newSQSClient(t *testing.T, ctx context.Context) *sqs.Client {
	t.Helper()
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(e2eAWSRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load local SQS config: %v", err)
	}
	client := sqs.NewFromConfig(awsCfg, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(getenv("OUTBOXER_E2E_SQS_ENDPOINT", e2eSQSEndpoint))
	})
	waitUntil(t, ctx, "ElasticMQ SQS", func(ctx context.Context) error {
		_, err := client.ListQueues(ctx, &sqs.ListQueuesInput{})
		return err
	})
	return client
}

func createSQSQueue(t *testing.T, ctx context.Context, client *sqs.Client, name string, fifo bool) string {
	t.Helper()
	attributes := map[string]string{"ReceiveMessageWaitTimeSeconds": "1"}
	if fifo {
		attributes["FifoQueue"] = "true"
		attributes["ContentBasedDeduplication"] = "false"
	}
	var queueURL string
	waitUntil(t, ctx, "sqs queue "+name, func(ctx context.Context) error {
		output, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName:  aws.String(name),
			Attributes: attributes,
		})
		if err != nil {
			return err
		}
		queueURL = aws.ToString(output.QueueUrl)
		return nil
	})
	return queueURL
}

func startOutboxer(t *testing.T, ctx context.Context, binary string, table string, overrides ...map[string]string) *runningProcess {
	t.Helper()
	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, binary)
	env := map[string]string{
		"EVENT_TABLE":             table,
		"PUBSUB_ENABLED":          "true",
		"SQS_ENABLED":             "true",
		"PUBSUB_PROJECT_ID":       e2eProjectID,
		"PUBSUB_EMULATOR_HOST":    getenv("OUTBOXER_E2E_PUBSUB_ENDPOINT", e2ePubSubEndpoint),
		"SQS_API_ENDPOINT":        getenv("OUTBOXER_E2E_SQS_ENDPOINT", e2eSQSEndpoint),
		"AWS_REGION":              e2eAWSRegion,
		"AWS_ACCESS_KEY_ID":       "test",
		"AWS_SECRET_ACCESS_KEY":   "test",
		"PG_HOST":                 getenv("OUTBOXER_E2E_PG_HOST", "localhost"),
		"PG_PORT":                 getenv("OUTBOXER_E2E_PG_PORT", "54329"),
		"PG_USER":                 getenv("OUTBOXER_E2E_PG_USER", "outboxer"),
		"PG_PASSWORD":             getenv("OUTBOXER_E2E_PG_PASSWORD", "outboxer"),
		"PG_DATABASE":             getenv("OUTBOXER_E2E_PG_DATABASE", "outboxer"),
		"PG_SSL":                  "false",
		"COLLECT_BATCH_TARGET":    "5000",
		"SQS_SEND_CONCURRENCY":    "4",
		"POLL_INTERVAL_MS":        "50",
		"ERROR_COOLDOWN_MS":       "50",
		"PUBLISH_TIMEOUT_MS":      "5000",
		"PUBLISH_RESULT_GRACE_MS": "500",
		"WATCHDOG_INTERVAL_MS":    "60000",
		"HEALTH_PORT":             "0",
		"LOG_LEVEL":               "info",
	}
	for _, override := range overrides {
		for key, value := range override {
			env[key] = value
		}
	}
	cmd.Env = mergedEnv(os.Environ(), env)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start outboxer: %v\n%s", err, output.String())
	}
	process := &runningProcess{cmd: cmd, output: &output}
	t.Cleanup(func() { stopOutboxer(t, process) })
	return process
}

func mergedEnv(base []string, overrides map[string]string) []string {
	env := make([]string, 0, len(base)+len(overrides))
	seen := map[string]struct{}{}
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if value, ok := overrides[key]; ok {
			env = append(env, key+"="+value)
			seen[key] = struct{}{}
			continue
		}
		env = append(env, entry)
		seen[key] = struct{}{}
	}
	for key, value := range overrides {
		if _, ok := seen[key]; ok {
			continue
		}
		env = append(env, key+"="+value)
	}
	return env
}

type runningProcess struct {
	cmd    *exec.Cmd
	output *bytes.Buffer
	once   sync.Once
}

func stopOutboxer(t *testing.T, process *runningProcess) {
	t.Helper()
	process.once.Do(func() {
		if process.cmd.Process == nil {
			return
		}
		_ = process.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- process.cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
				t.Logf("outboxer exited with %v\n%s", err, process.output.String())
			}
		case <-time.After(5 * time.Second):
			_ = process.cmd.Process.Kill()
			t.Fatalf("outboxer did not stop\n%s", process.output.String())
		}
	})
}

func receivePubSubMessages(t *testing.T, ctx context.Context, client *pubsub.Client, subscriptionID string, want int) []pubsubReceivedMessage {
	t.Helper()
	receiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	subscription := client.Subscriber(subscriptionID)
	subscription.ReceiveSettings.NumGoroutines = 1
	subscription.ReceiveSettings.MaxOutstandingMessages = 100

	var mu sync.Mutex
	messages := []pubsubReceivedMessage{}
	done := make(chan error, 1)
	go func() {
		done <- subscription.Receive(receiveCtx, func(_ context.Context, msg *pubsub.Message) {
			mu.Lock()
			messages = append(messages, pubsubReceivedMessage{
				body:        string(msg.Data),
				orderingKey: msg.OrderingKey,
				attributes:  copyStringMap(msg.Attributes),
			})
			if len(messages) >= want {
				cancel()
			}
			mu.Unlock()
			msg.Ack()
		})
	}()

	select {
	case err := <-done:
		if err != nil && receiveCtx.Err() == nil {
			t.Fatalf("receive Pub/Sub %s: %v", subscriptionID, err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out receiving Pub/Sub messages for %s: got %d, want %d", subscriptionID, len(messages), want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(messages) != want {
		t.Fatalf("received %d Pub/Sub messages for %s, want %d: %#v", len(messages), subscriptionID, want, messages)
	}
	return append([]pubsubReceivedMessage(nil), messages...)
}

func receiveSQSMessages(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string, want int) []sqsReceivedMessage {
	t.Helper()
	messages := []sqsReceivedMessage{}
	waitUntil(t, ctx, "sqs messages "+queueURL, func(ctx context.Context) error {
		output, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(queueURL),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll},
			VisibilityTimeout:           5,
		})
		if err != nil {
			return err
		}
		for _, msg := range output.Messages {
			messages = append(messages, sqsReceivedMessage{
				body:       aws.ToString(msg.Body),
				groupID:    msg.Attributes["MessageGroupId"],
				attributes: sqsStringAttributes(msg.MessageAttributes),
			})
			_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				return err
			}
		}
		if len(messages) < want {
			return fmt.Errorf("received %d of %d messages", len(messages), want)
		}
		return nil
	})
	if len(messages) != want {
		t.Fatalf("received %d SQS messages from %s, want %d: %#v", len(messages), queueURL, want, messages)
	}
	return messages
}

func waitForEmptyTable(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	waitForTableCount(t, ctx, db, table, 0)
}

func waitForTableCount(t *testing.T, ctx context.Context, db *sql.DB, table string, want int) {
	t.Helper()
	waitUntil(t, ctx, fmt.Sprintf("event table count %d", want), func(ctx context.Context) error {
		var remaining int
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", ident(table))).Scan(&remaining); err != nil {
			return err
		}
		if remaining != want {
			return fmt.Errorf("%d events remain, want %d", remaining, want)
		}
		return nil
	})
}

func assertBodies[T interface{ bodyValue() string }](t *testing.T, messages []T, prefix string, count int) {
	t.Helper()
	got := make([]string, 0, len(messages))
	for _, message := range messages {
		got = append(got, message.bodyValue())
	}
	sort.Strings(got)
	want := make([]string, count)
	for i := range want {
		want[i] = fmt.Sprintf("%s%02d", prefix, i)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected bodies for %s:\ngot  %#v\nwant %#v", prefix, got, want)
	}
}

func (m pubsubReceivedMessage) bodyValue() string { return m.body }
func (m sqsReceivedMessage) bodyValue() string    { return m.body }

func assertPubSubOrdering(t *testing.T, messages []pubsubReceivedMessage, key string, count int) {
	t.Helper()
	got := []string{}
	for _, message := range messages {
		if message.orderingKey == key {
			got = append(got, message.body)
			if message.attributes["key"] != key {
				t.Fatalf("expected Pub/Sub key attribute %q, got %#v", key, message.attributes)
			}
		}
	}
	assertOrderedBodies(t, got, "pubsub-ordered-"+key+"-", count)
}

func assertSQSOrdering(t *testing.T, messages []sqsReceivedMessage, group string, count int) {
	t.Helper()
	got := []string{}
	for _, message := range messages {
		if message.groupID == group {
			got = append(got, message.body)
			if message.attributes["group"] != group {
				t.Fatalf("expected SQS group attribute %q, got %#v", group, message.attributes)
			}
		}
	}
	assertOrderedBodies(t, got, "sqs-fifo-"+group+"-", count)
}

func assertOrderedBodies(t *testing.T, got []string, prefix string, count int) {
	t.Helper()
	want := make([]string, count)
	for i := range want {
		want[i] = fmt.Sprintf("%s%02d", prefix, i)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected ordered bodies for %s:\ngot  %#v\nwant %#v", prefix, got, want)
	}
}

func waitUntil(t *testing.T, ctx context.Context, name string, fn func(context.Context) error) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = fn(callCtx)
		cancel()
		if lastErr == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", name, lastErr)
		case <-ticker.C:
		}
	}
}

func sqsStringAttributes(attributes map[string]sqstypes.MessageAttributeValue) map[string]string {
	out := map[string]string{}
	for key, value := range attributes {
		out[key] = aws.ToString(value.StringValue)
	}
	return out
}

func copyStringMap(values map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range values {
		out[key] = value
	}
	return out
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func ident(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
