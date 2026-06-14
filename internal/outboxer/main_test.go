package outboxer

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakePubSubPublisher struct {
	mu       sync.Mutex
	err      error
	messages []pubsubMessage
}

func (p *fakePubSubPublisher) Publish(_ context.Context, message pubsubMessage) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	if p.err != nil {
		return "", p.err
	}
	return fmt.Sprintf("published-%d", len(p.messages)), nil
}

type fakeSQSPublisher struct {
	mu        sync.Mutex
	err       error
	response  sqsBatchResponse
	requests  []fakeSQSRequest
	autoReply bool
}

type fakeSQSRequest struct {
	queueURL string
	entries  []sqsBatchEntry
}

func (p *fakeSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	copiedEntries := append([]sqsBatchEntry(nil), entries...)
	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: copiedEntries})
	if p.err != nil {
		return sqsBatchResponse{}, p.err
	}
	if p.autoReply {
		response := sqsBatchResponse{}
		for _, entry := range entries {
			response.Successful = append(response.Successful, sqsBatchSuccess{
				ID:        entry.ID,
				MessageID: "message-" + entry.ID,
			})
		}
		return response, nil
	}
	return p.response, nil
}

func testConfig() appConfig {
	return appConfig{
		EventTable:       "events",
		EventID:          "id",
		EventTimestamp:   "timestamp",
		EventPayload:     "payload",
		EventTarget:      "target",
		EventDestination: "destination",
		EventOrderingKey: "ordering_key",
		EventAttributes:  "attributes",

		BatchSize:          32,
		BatchWorkers:       4,
		BatchMaxSequential: 8,

		WatchdogInterval:   time.Hour,
		HealthPort:         9999,
		PubSubEnabled:      true,
		SQSEnabled:         true,
		DefaultPubSubTopic: "default",
		ErrorCooldown:      time.Millisecond,
	}
}

func unsetEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		original, existed := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, original)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}

func TestLoadConfigUsesDefaults(t *testing.T) {
	unsetEnv(t, "EVENT_PAYLOAD", "EVENT_DESTINATION", "PG_HOST", "PG_USER", "HEALTH_PORT", "PORT", "POLL_INTERVAL_MS", "WATCHDOG_INTERVAL_MS")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.PGHost != "localhost" {
		t.Fatalf("expected default pg host, got %q", cfg.PGHost)
	}
	if cfg.PGUser != "postgres" {
		t.Fatalf("expected default pg user, got %q", cfg.PGUser)
	}
	if cfg.EventPayload != "payload" {
		t.Fatalf("expected default event payload column, got %q", cfg.EventPayload)
	}
	if cfg.EventDestination != "destination" {
		t.Fatalf("expected default event destination column, got %q", cfg.EventDestination)
	}
	if cfg.HealthPort != 0 {
		t.Fatalf("expected default healthcheck port 0, got %d", cfg.HealthPort)
	}
	if cfg.PollInterval != 0 {
		t.Fatalf("expected default poll interval 0, got %s", cfg.PollInterval)
	}
	if cfg.WatchdogInterval != 10*time.Minute {
		t.Fatalf("expected default watchdog interval 10m, got %s", cfg.WatchdogInterval)
	}
}

func TestLoadConfigUsesEnv(t *testing.T) {
	t.Setenv("PG_HOST", "db")
	t.Setenv("POLL_INTERVAL_MS", "250")
	t.Setenv("PORT", "9090")
	t.Setenv("HEALTH_PORT", "")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.PGHost != "db" {
		t.Fatalf("expected env pg host, got %q", cfg.PGHost)
	}
	if cfg.PollInterval != 250*time.Millisecond {
		t.Fatalf("expected env poll interval, got %s", cfg.PollInterval)
	}
	if cfg.HealthPort != 9090 {
		t.Fatalf("expected PORT fallback, got %d", cfg.HealthPort)
	}
}

func TestLoadConfigHealthPortPrecedence(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("HEALTH_PORT", "9000")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.HealthPort != 9000 {
		t.Fatalf("expected HEALTH_PORT to override PORT, got %d", cfg.HealthPort)
	}

	cfg, err = loadConfig([]string{"--health-port=0"}, io.Discard)
	if err != nil {
		t.Fatalf("load config with flag: %v", err)
	}
	if cfg.HealthPort != 0 {
		t.Fatalf("expected CLI flag to disable healthcheck server, got %d", cfg.HealthPort)
	}
}

func TestLoadConfigFlagsOverrideEnv(t *testing.T) {
	t.Setenv("EVENT_PAYLOAD", "env_payload")
	t.Setenv("EVENT_DESTINATION", "env_destination")
	t.Setenv("PG_HOST", "env-db")
	t.Setenv("PG_PORT", "5433")
	t.Setenv("PG_CONNECT_TIMEOUT_MS", "1000")
	t.Setenv("PG_SSL", "false")
	t.Setenv("POLL_INTERVAL_MS", "250")
	t.Setenv("WATCHDOG_INTERVAL_MS", "60000")

	cfg, err := loadConfig([]string{
		"--event-payload=flag_payload",
		"--event-destination=flag_destination",
		"--pg-host=flag-db",
		"--pg-port=6543",
		"--pg-connect-timeout-ms=2000",
		"--pg-ssl=true",
		"--poll-interval-ms=500",
		"--watchdog-interval-ms=30000",
	}, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.EventPayload != "flag_payload" {
		t.Fatalf("expected flag event payload column, got %q", cfg.EventPayload)
	}
	if cfg.EventDestination != "flag_destination" {
		t.Fatalf("expected flag event destination column, got %q", cfg.EventDestination)
	}
	if cfg.PGHost != "flag-db" {
		t.Fatalf("expected flag pg host, got %q", cfg.PGHost)
	}
	if cfg.PGPort != 6543 {
		t.Fatalf("expected flag pg port, got %d", cfg.PGPort)
	}
	if !cfg.PGSSL {
		t.Fatal("expected flag pg ssl to override env")
	}
	if cfg.PGConnectTimeout != 2*time.Second {
		t.Fatalf("expected flag pg connect timeout, got %s", cfg.PGConnectTimeout)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Fatalf("expected flag poll interval, got %s", cfg.PollInterval)
	}
	if cfg.WatchdogInterval != 30*time.Second {
		t.Fatalf("expected flag watchdog interval, got %s", cfg.WatchdogInterval)
	}
}

func TestLoadConfigHelpMentionsEnvVars(t *testing.T) {
	t.Setenv("PG_PASSWORD", "super-secret")

	var output bytes.Buffer
	_, err := loadConfig([]string{"--help"}, &output)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("expected flag.ErrHelp, got %v", err)
	}

	help := output.String()
	for _, expected := range []string{
		"Usage:",
		"Event table:",
		"Batch processing:",
		"HTTP / health:",
		"PostgreSQL:",
		"Google Pub/Sub:",
		"AWS SQS:",
		"--event-payload",
		"Env: EVENT_PAYLOAD",
		"--event-destination",
		"Env: EVENT_DESTINATION",
		"--health-port",
		"Env: HEALTH_PORT, PORT",
		"--pg-host",
		"Env: PG_HOST",
		"--pg-connect-timeout-ms",
		"Env: PG_CONNECT_TIMEOUT_MS",
		"--poll-interval-ms",
		"Env: POLL_INTERVAL_MS",
		"--watchdog-interval-ms",
		"Env: WATCHDOG_INTERVAL_MS",
		"--aws-role-session-name",
		"Env: AWS_ROLE_SESSION_NAME",
	} {
		if !strings.Contains(help, expected) {
			t.Fatalf("expected help to contain %q, got:\n%s", expected, help)
		}
	}
	if strings.Contains(help, "super-secret") {
		t.Fatalf("expected help to redact database password, got:\n%s", help)
	}
	if !strings.Contains(help, "Env: PG_PASSWORD") || !strings.Contains(help, "Default: <set>") {
		t.Fatalf("expected help to mention redacted pg password default, got:\n%s", help)
	}
}

func TestValidateRequiresAnEnabledBackend(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.SQSEnabled = false
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when no backend is enabled")
	}

	cfg.PubSubEnabled = true
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected single enabled backend to be valid, got %v", err)
	}
}

func TestValidateRequiresTargetColumnWhenBothEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = true
	cfg.SQSEnabled = true

	cfg.EventTarget = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when both backends are enabled without a target column")
	}

	cfg.EventTarget = "target"
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected both backends with a target column to be valid, got %v", err)
	}
}

func TestValidateRequiresIDAndPayloadColumns(t *testing.T) {
	cfg := testConfig()
	cfg.EventID = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when id column is empty")
	}

	cfg = testConfig()
	cfg.EventPayload = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when payload column is empty")
	}
}

func TestValidateRequiresDestinationOrDefault(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = true
	cfg.SQSEnabled = false
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when Pub/Sub has neither a destination column nor a default topic")
	}
	cfg.DefaultPubSubTopic = "default"
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected a default topic to satisfy Pub/Sub destination, got %v", err)
	}

	cfg = testConfig()
	cfg.PubSubEnabled = false
	cfg.SQSEnabled = true
	cfg.EventDestination = ""
	cfg.DefaultSQSQueueURL = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when SQS has neither a destination column nor a default queue URL")
	}
	cfg.DefaultSQSQueueURL = "https://sqs.example/q"
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected a default queue URL to satisfy SQS destination, got %v", err)
	}
}

func TestCheckRequiredColumns(t *testing.T) {
	base := testConfig()

	cases := []struct {
		name    string
		mutate  func(*appConfig)
		columns []string
		wantErr bool
	}{
		{"both enabled needs target and destination", nil, []string{"id", "payload", "target", "destination"}, false},
		{"missing id", nil, []string{"payload", "target", "destination"}, true},
		{"missing target when both enabled", nil, []string{"id", "payload", "destination"}, true},
		{
			"destination optional once both defaults cover it",
			func(c *appConfig) { c.DefaultPubSubTopic = "default"; c.DefaultSQSQueueURL = "https://sqs.example/q" },
			[]string{"id", "payload", "target"},
			false,
		},
		{
			"pubsub only without default needs destination",
			func(c *appConfig) { c.SQSEnabled = false; c.DefaultPubSubTopic = "" },
			[]string{"id", "payload"},
			true,
		},
		{
			"pubsub only with default skips optional columns",
			func(c *appConfig) { c.SQSEnabled = false },
			[]string{"id", "payload"},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			a := &app{cfg: cfg}
			err := a.checkRequiredColumns(tc.columns)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestSendSQSEventsUsesDefaultQueueURL(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.DefaultSQSQueueURL = "https://sqs.example/default"
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "payload": "one"}},
	}

	if err := a.sendSQSEvents(context.Background(), nil, events, func(id any) { deleted = append(deleted, id) }); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 1 || sqs.requests[0].queueURL != "https://sqs.example/default" {
		t.Fatalf("expected request to default queue URL, got %#v", sqs.requests)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestResolveBackendRouting(t *testing.T) {
	bothEnabled := testConfig()

	pubsubOnly := testConfig()
	pubsubOnly.SQSEnabled = false

	sqsOnly := testConfig()
	sqsOnly.PubSubEnabled = false

	newEvent := func(target string) event {
		columns := map[string]any{"id": "event-1"}
		if target != "" {
			columns["target"] = target
		}
		return event{columns: columns}
	}

	cases := []struct {
		name   string
		cfg    appConfig
		target string
		want   backend
	}{
		{"both: explicit pubsub", bothEnabled, "pubsub", backendPubSub},
		{"both: explicit sqs", bothEnabled, "sqs", backendSQS},
		{"both: empty target is ambiguous", bothEnabled, "", backendNone},
		{"both: unknown target", bothEnabled, "kafka", backendNone},
		{"pubsub only: empty target routes to pubsub", pubsubOnly, "", backendPubSub},
		{"pubsub only: explicit sqs is unroutable", pubsubOnly, "sqs", backendNone},
		{"sqs only: empty target routes to sqs", sqsOnly, "", backendSQS},
		{"sqs only: explicit pubsub is unroutable", sqsOnly, "pubsub", backendNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &app{cfg: tc.cfg}
			if got := a.resolveBackend(newEvent(tc.target)); got != tc.want {
				t.Fatalf("resolveBackend(%q) = %v, want %v", tc.target, got, tc.want)
			}
		})
	}
}

func TestParallelizeEventsKeepsOrderingKeysSequentialAndCapsLongJobs(t *testing.T) {
	cfg := testConfig()
	cfg.BatchWorkers = 1
	cfg.BatchMaxSequential = 2

	events := []event{
		{columns: map[string]any{"ordering_key": "account-1", "id": 1}},
		{columns: map[string]any{"ordering_key": "account-1", "id": 2}},
		{columns: map[string]any{"ordering_key": "account-1", "id": 3}},
	}

	jobs := parallelizeEvents(cfg, events)
	if got := len(jobs[0]); got != 2 {
		t.Fatalf("expected capped ordered job length 2, got %d", got)
	}
	if got := eventValue(jobs[0][0], "id"); got != 1 {
		t.Fatalf("expected first ordered event to stay first, got %v", got)
	}
	if got := eventValue(jobs[0][1], "id"); got != 2 {
		t.Fatalf("expected second ordered event to stay second, got %v", got)
	}
}

func TestSendPubsubEventUsesDefaultTopicAndSanitizesAttributes(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	evt := event{columns: map[string]any{
		"id":         "event-1",
		"payload":    "payload",
		"attributes": []byte(`{"keep":"yes","drop":42}`),
	}}

	err := a.sendPubsubEvent(context.Background(), nil, evt, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendPubsubEvent returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(pubsub.messages))
	}

	message := pubsub.messages[0]
	if message.Topic != "default" {
		t.Fatalf("expected default topic, got %q", message.Topic)
	}
	if string(message.Data) != "payload" {
		t.Fatalf("expected payload body, got %q", string(message.Data))
	}
	if !reflect.DeepEqual(message.Attributes, map[string]string{"keep": "yes"}) {
		t.Fatalf("unexpected attributes: %#v", message.Attributes)
	}
}

func TestSendPubsubEventReturnsPublisherError(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("pubsub unavailable")
	a := &app{cfg: cfg, pubsub: &fakePubSubPublisher{err: expectedErr}}

	evt := event{columns: map[string]any{
		"id":          "event-1",
		"destination": "topic-1",
		"payload":     "payload",
	}}

	err := a.sendPubsubEvent(context.Background(), nil, evt, func(any) {})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected publisher error, got %v", err)
	}
}

func TestSendSQS10EventsHandlesPartialResponsesAndFIFOGroupIDs(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{response: sqsBatchResponse{
		Successful: []sqsBatchSuccess{{ID: "event-1", MessageID: "message-1"}},
		Failed: []sqsBatchFailure{
			{ID: "event-2", Code: "InvalidMessageContents", Message: "bad", SenderFault: true},
			{ID: "event-3", Code: "InternalError", Message: "later", SenderFault: false},
		},
	}}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "ordering_key": "group-a", "attributes": []byte(`{"ok":"1","bad":true}`)}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a", "payload": "three", "ordering_key": "group-a"}},
	}

	err := a.sendSQS10Events(context.Background(), nil, "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected one SQS request, got %d", len(sqs.requests))
	}
	for _, entry := range sqs.requests[0].entries {
		if entry.MessageGroupID != "group-a" {
			t.Fatalf("expected FIFO message group id, got %q", entry.MessageGroupID)
		}
	}
	if !reflect.DeepEqual(sqs.requests[0].entries[0].Attributes, map[string]string{"ok": "1"}) {
		t.Fatalf("unexpected sanitized attributes: %#v", sqs.requests[0].entries[0].Attributes)
	}
}

func TestPostgresIntegrationProcessesAndDeletesEvents(t *testing.T) {
	dsn := os.Getenv("OUTBOXER_INTEGRATION_PG_DSN")
	if dsn == "" {
		t.Skip("set OUTBOXER_INTEGRATION_PG_DSN to run the Postgres integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	table := "outboxer_test_" + strings.ReplaceAll(strconvNano(), "-", "_")
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id text PRIMARY KEY,
			timestamp timestamptz,
			payload text NOT NULL,
			target text,
			destination text,
			ordering_key text,
			attributes jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	defer db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, timestamp, payload, target, destination, ordering_key, attributes)
		VALUES
			('pubsub-1', now(), 'hello pubsub', 'pubsub', 'topic-a', null, '{"trace":"abc"}'),
			('sqs-1', now(), 'hello sqs', 'sqs', 'queue-a', null, '{"trace":"def"}')
	`, ident(table)))
	if err != nil {
		t.Fatalf("insert events: %v", err)
	}

	cfg := testConfig()
	cfg.EventTable = table
	cfg.BatchWorkers = 2
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, db: db, pubsub: pubsub, sqs: sqs}

	result, err := a.processOneBatch(ctx)
	if err != nil {
		t.Fatalf("process events: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected 2 selected events, got %d", result.selected)
	}

	var remaining int
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", ident(table))).Scan(&remaining); err != nil {
		t.Fatalf("count remaining events: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected all events deleted, got %d remaining", remaining)
	}

	if len(pubsub.messages) != 1 {
		t.Fatalf("expected one pubsub message, got %d", len(pubsub.messages))
	}
	if len(sqs.requests) != 1 || len(sqs.requests[0].entries) != 1 {
		t.Fatalf("expected one sqs message, got %#v", sqs.requests)
	}

	gotBodies := []string{string(pubsub.messages[0].Data), sqs.requests[0].entries[0].MessageBody}
	sort.Strings(gotBodies)
	if !reflect.DeepEqual(gotBodies, []string{"hello pubsub", "hello sqs"}) {
		t.Fatalf("unexpected published bodies: %#v", gotBodies)
	}

	result, err = a.processOneBatch(ctx)
	if err != nil {
		t.Fatalf("process empty batch: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected empty batch to select 0 events, got %d", result.selected)
	}
}

func TestHealthcheckReturnsOK(t *testing.T) {
	a := &app{cfg: testConfig()}
	server := a.newHTTPServer()

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.Code)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(body) != "all good" {
		t.Fatalf("expected healthcheck body, got %q", string(body))
	}
}

func strconvNano() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
