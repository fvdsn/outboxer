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
		EventData:        "data",
		EventTarget:      "target",
		EventTopic:       "topic",
		EventOrderingKey: "ordering_key",
		EventAttributes:  "attributes",

		BatchSize:          32,
		BatchWorkers:       4,
		BatchMaxSequential: 8,

		DeadlockCheckInterval: time.Hour,
		HealthcheckPort:       9999,
		DefaultTopic:          "default",
		ErrorCooldown:         time.Millisecond,
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
	unsetEnv(t, "PG_HOST", "PG_USER", "HEALTHCHECK_PORT", "PORT", "POLL_INTERVAL_MS")

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
	if cfg.HealthcheckPort != 8080 {
		t.Fatalf("expected default healthcheck port 8080, got %d", cfg.HealthcheckPort)
	}
	if cfg.PollInterval != 0 {
		t.Fatalf("expected default poll interval 0, got %s", cfg.PollInterval)
	}
}

func TestLoadConfigUsesEnv(t *testing.T) {
	t.Setenv("PG_HOST", "db")
	t.Setenv("POLL_INTERVAL_MS", "250")
	t.Setenv("PORT", "9090")
	t.Setenv("HEALTHCHECK_PORT", "")

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
	if cfg.HealthcheckPort != 9090 {
		t.Fatalf("expected PORT fallback, got %d", cfg.HealthcheckPort)
	}
}

func TestLoadConfigFlagsOverrideEnv(t *testing.T) {
	t.Setenv("PG_HOST", "env-db")
	t.Setenv("PG_PORT", "5433")
	t.Setenv("PG_SSL", "false")
	t.Setenv("POLL_INTERVAL_MS", "250")

	cfg, err := loadConfig([]string{
		"--pg-host=flag-db",
		"--pg-port=6543",
		"--pg-ssl=true",
		"--poll-interval-ms=500",
	}, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
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
	if cfg.PollInterval != 500*time.Millisecond {
		t.Fatalf("expected flag poll interval, got %s", cfg.PollInterval)
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
		"--pg-host",
		"Env: PG_HOST",
		"--poll-interval-ms",
		"Env: POLL_INTERVAL_MS",
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
		"data":       "payload",
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
		"id":    "event-1",
		"topic": "topic-1",
		"data":  "payload",
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
		{columns: map[string]any{"id": "event-1", "topic": "queue-a", "data": "one", "ordering_key": "group-a", "attributes": []byte(`{"ok":"1","bad":true}`)}},
		{columns: map[string]any{"id": "event-2", "topic": "queue-a", "data": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "topic": "queue-a", "data": "three", "ordering_key": "group-a"}},
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
			data text NOT NULL,
			target text,
			topic text,
			ordering_key text,
			attributes jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	defer db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, timestamp, data, target, topic, ordering_key, attributes)
		VALUES
			('pubsub-1', now(), 'hello pubsub', null, 'topic-a', null, '{"trace":"abc"}'),
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
