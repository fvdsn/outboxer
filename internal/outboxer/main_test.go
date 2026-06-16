package outboxer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/DATA-DOG/go-sqlmock"
)

type fakePubSubPublisher struct {
	mu       sync.Mutex
	err      error
	errs     []error
	results  []fakePubSubResult
	messages []pubsubMessage
	flushes  []string
	resumes  []fakePubSubResume
}

type fakePubSubResume struct {
	topic       string
	orderingKey string
}

type fakePubSubResult struct {
	messageID string
	err       error
	block     bool
	delay     time.Duration
}

func (p *fakePubSubPublisher) Publish(_ context.Context, message pubsubMessage) pubsubPublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	if len(p.results) > 0 {
		result := p.results[0]
		p.results = p.results[1:]
		if result.messageID == "" {
			result.messageID = fmt.Sprintf("published-%d", len(p.messages))
		}
		return result
	}
	err := p.err
	if len(p.errs) > 0 {
		err = p.errs[0]
		p.errs = p.errs[1:]
	}
	return fakePubSubResult{messageID: fmt.Sprintf("published-%d", len(p.messages)), err: err}
}

func (p *fakePubSubPublisher) Flush(topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushes = append(p.flushes, topic)
}

func (p *fakePubSubPublisher) ResumePublish(topic string, orderingKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes = append(p.resumes, fakePubSubResume{topic: topic, orderingKey: orderingKey})
}

func (p *fakePubSubPublisher) Close() error {
	return nil
}

func (r fakePubSubResult) Get(ctx context.Context) (string, error) {
	if r.block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if r.delay > 0 {
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	if r.err != nil {
		return "", r.err
	}
	return r.messageID, nil
}

type fakeSQSPublisher struct {
	mu        sync.Mutex
	err       error
	errs      []error
	response  sqsBatchResponse
	responses []sqsBatchResponse
	requests  []fakeSQSRequest
	autoReply bool
}

type fakeSQSRequest struct {
	queueURL string
	entries  []sqsBatchEntry
}

type fakeSQSAPIError struct {
	code string
}

func (e fakeSQSAPIError) Error() string {
	return e.code
}

func (e fakeSQSAPIError) ErrorCode() string {
	return e.code
}

func (p *fakeSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	copiedEntries := append([]sqsBatchEntry(nil), entries...)
	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: copiedEntries})
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]
		if err != nil {
			return sqsBatchResponse{}, err
		}
	}
	if p.err != nil {
		return sqsBatchResponse{}, p.err
	}
	if len(p.responses) > 0 {
		response := p.responses[0]
		p.responses = p.responses[1:]
		return response, nil
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

type keyedSQSPublisher struct {
	mu        sync.Mutex
	requests  []fakeSQSRequest
	responses map[string]sqsBatchResponse
}

func (p *keyedSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]sqsBatchEntry(nil), entries...)})
	if len(entries) == 0 {
		return sqsBatchResponse{}, nil
	}
	return p.responses[entries[0].ID], nil
}

type trackingSQSPublisher struct {
	mu       sync.Mutex
	inFlight int
	max      int
	started  chan struct{}
	release  chan struct{}
	requests []fakeSQSRequest
}

func (p *trackingSQSPublisher) SendBatch(_ context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	p.mu.Lock()
	p.inFlight++
	if p.inFlight > p.max {
		p.max = p.inFlight
	}
	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]sqsBatchEntry(nil), entries...)})
	p.mu.Unlock()

	p.started <- struct{}{}
	<-p.release

	p.mu.Lock()
	p.inFlight--
	p.mu.Unlock()

	response := sqsBatchResponse{}
	for _, entry := range entries {
		response.Successful = append(response.Successful, sqsBatchSuccess{
			ID:        entry.ID,
			MessageID: "message-" + entry.ID,
		})
	}
	return response, nil
}

type trackingPubSubPublisher struct {
	mu       sync.Mutex
	started  chan struct{}
	release  chan struct{}
	messages []pubsubMessage
}

type trackingPubSubResult struct {
	started chan struct{}
	release chan struct{}
}

func (p *trackingPubSubPublisher) Publish(_ context.Context, message pubsubMessage) pubsubPublishResult {
	p.mu.Lock()
	p.messages = append(p.messages, message)
	p.mu.Unlock()
	return trackingPubSubResult{started: p.started, release: p.release}
}

func (p *trackingPubSubPublisher) Flush(string) {}

func (p *trackingPubSubPublisher) ResumePublish(string, string) {}

func (p *trackingPubSubPublisher) Close() error {
	return nil
}

func (r trackingPubSubResult) Get(context.Context) (string, error) {
	r.started <- struct{}{}
	<-r.release
	return "message-1", nil
}

type concurrentPubSubPublisher struct {
	mu       sync.Mutex
	messages []pubsubMessage
	started  chan string
	release  chan struct{}
}

type concurrentPubSubResult struct {
	key     string
	started chan string
	release chan struct{}
}

func (p *concurrentPubSubPublisher) Publish(_ context.Context, message pubsubMessage) pubsubPublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	return concurrentPubSubResult{key: message.OrderingKey, started: p.started, release: p.release}
}

func (p *concurrentPubSubPublisher) Flush(string) {}

func (p *concurrentPubSubPublisher) ResumePublish(string, string) {}

func (p *concurrentPubSubPublisher) Close() error {
	return nil
}

func (r concurrentPubSubResult) Get(context.Context) (string, error) {
	r.started <- r.key
	<-r.release
	return "message-" + r.key, nil
}

type fakeTopicPublisher struct {
	mu        sync.Mutex
	publishes int
	flushes   int
	resumes   []string
	stopCount int
}

func (p *fakeTopicPublisher) Publish(context.Context, *pubsub.Message) *pubsub.PublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishes++
	return nil
}

func (p *fakeTopicPublisher) Flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushes++
}

func (p *fakeTopicPublisher) ResumePublish(orderingKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes = append(p.resumes, orderingKey)
}

func (p *fakeTopicPublisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopCount++
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

		BatchSize:            32,
		SQSSendConcurrency:   8,
		OrderedGroupBatchCap: 8,

		WatchdogInterval:   time.Hour,
		HealthPort:         9999,
		PubSubEnabled:      true,
		SQSEnabled:         true,
		DefaultPubSubTopic: "default",
		ErrorCooldown:      time.Millisecond,
		PublishTimeout:     30 * time.Second,
		PublishResultGrace: 5 * time.Second,
	}
}

const (
	selectEventsSQL = `SELECT * FROM "events" ORDER BY "id" LIMIT $1 FOR UPDATE`
	deleteOneSQL    = `DELETE FROM "events" WHERE "id" IN ($1)`
	deleteTwoSQL    = `DELETE FROM "events" WHERE "id" IN ($1, $2)`
)

func newMockProcessorApp(t *testing.T, cfg appConfig) (*app, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("open sql mock: %v", err)
	}
	cleanup := func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("sql expectations: %v", err)
		}
		_ = db.Close()
	}
	return &app{cfg: cfg, db: db, failureLogger: newFailureLogger(time.Minute)}, mock, cleanup
}

func mockEventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "target", "destination", "payload", "ordering_key", "attributes"})
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

func TestLoadConfigVerifiesTLSByDefault(t *testing.T) {
	unsetEnv(t, "PG_SSL_REJECT_UNAUTHORIZED")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.PGSSLRejectUnauthorized {
		t.Fatal("expected TLS certificate verification to be enabled by default")
	}

	cfg, err = loadConfig([]string{"--pg-ssl-reject-unauthorized=false"}, io.Discard)
	if err != nil {
		t.Fatalf("load config with flag: %v", err)
	}
	if cfg.PGSSLRejectUnauthorized {
		t.Fatal("expected flag to disable TLS verification")
	}
}

func TestBuildTLSConfig(t *testing.T) {
	cfg := testConfig()
	cfg.PGHost = "db.example.com"
	cfg.PGSSLRejectUnauthorized = true

	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig.ServerName != "db.example.com" {
		t.Fatalf("expected ServerName from host, got %q", tlsConfig.ServerName)
	}
	if tlsConfig.InsecureSkipVerify {
		t.Fatal("expected verification enabled when reject-unauthorized is true")
	}

	cfg.PGSSLRejectUnauthorized = false
	tlsConfig, err = buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Fatal("expected verification skipped when reject-unauthorized is false")
	}
}

func TestBuildTLSConfigRootCert(t *testing.T) {
	cfg := testConfig()

	cfg.PGSSLRootCert = filepath.Join(t.TempDir(), "missing.pem")
	if _, err := buildTLSConfig(cfg); err == nil {
		t.Fatal("expected error for a missing root cert file")
	}

	invalid := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(invalid, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write invalid cert: %v", err)
	}
	cfg.PGSSLRootCert = invalid
	if _, err := buildTLSConfig(cfg); err == nil {
		t.Fatal("expected error for a file with no certificates")
	}

	cfg.PGSSLRootCert = writeTestCACert(t)
	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildTLSConfig with valid cert: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("expected RootCAs to be set from the root cert")
	}
}

func writeTestCACert(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "outboxer-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	return path
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
		"--sqs-send-concurrency",
		"Env: SQS_SEND_CONCURRENCY",
		"--ordered-group-batch-cap",
		"Env: ORDERED_GROUP_BATCH_CAP",
		"--publish-result-grace-ms",
		"Env: PUBLISH_RESULT_GRACE_MS",
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

	if err := a.sendSQSEvents(context.Background(), events, func(id any) { deleted = append(deleted, id) }); err != nil {
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
		columns := map[string]any{"id": "event-1", "destination": "dest-1"}
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

func TestClassifyRouteFailures(t *testing.T) {
	bothEnabled := testConfig()

	pubsubOnly := testConfig()
	pubsubOnly.SQSEnabled = false

	sqsOnly := testConfig()
	sqsOnly.PubSubEnabled = false

	newEvent := func(target string, destination string) event {
		columns := map[string]any{"id": "event-1"}
		if target != "" {
			columns["target"] = target
		}
		if destination != "" {
			columns["destination"] = destination
		}
		return event{columns: columns}
	}

	cases := []struct {
		name        string
		cfg         appConfig
		evt         event
		wantBackend backend
		wantFailure routingFailure
	}{
		{"target pubsub enabled", bothEnabled, newEvent("pubsub", "topic-a"), backendPubSub, routingFailureNone},
		{"target sqs enabled", bothEnabled, newEvent("sqs", "queue-a"), backendSQS, routingFailureNone},
		{"target pubsub disabled", sqsOnly, newEvent("pubsub", ""), backendNone, routingFailureDisabled},
		{"target sqs disabled", pubsubOnly, newEvent("sqs", ""), backendNone, routingFailureDisabled},
		{"empty target one backend", pubsubOnly, newEvent("", "topic-a"), backendPubSub, routingFailureNone},
		{"empty target both backends", bothEnabled, newEvent("", "topic-a"), backendNone, routingFailureAmbiguous},
		{"unknown target", bothEnabled, newEvent("kafka", "topic-a"), backendNone, routingFailureUnsupported},
		{"empty destination no default", pubsubOnlyNoDefault(), newEvent("pubsub", ""), backendNone, routingFailureNoDestination},
		{"disabled backend before destination", pubsubOnly, newEvent("sqs", ""), backendNone, routingFailureDisabled},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &app{cfg: tc.cfg}
			got := a.classifyRoute(tc.evt)
			if got.backend != tc.wantBackend || got.failure != tc.wantFailure {
				t.Fatalf("classifyRoute() = {backend:%v failure:%q}, want {backend:%v failure:%q}", got.backend, got.failure, tc.wantBackend, tc.wantFailure)
			}
		})
	}
}

func pubsubOnlyNoDefault() appConfig {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.DefaultPubSubTopic = ""
	return cfg
}

type blockingPubSubPublisher struct{}

func (blockingPubSubPublisher) Publish(_ context.Context, _ pubsubMessage) pubsubPublishResult {
	return fakePubSubResult{block: true}
}

func (blockingPubSubPublisher) Flush(string) {}

func (blockingPubSubPublisher) ResumePublish(string, string) {}

func (blockingPubSubPublisher) Close() error {
	return nil
}

type blockingSQSPublisher struct{}

func (blockingSQSPublisher) SendBatch(ctx context.Context, _ string, _ []sqsBatchEntry) (sqsBatchResponse, error) {
	<-ctx.Done()
	return sqsBatchResponse{}, ctx.Err()
}

type recordingBlockingSQSPublisher struct {
	mu       sync.Mutex
	requests []fakeSQSRequest
}

func (p *recordingBlockingSQSPublisher) SendBatch(ctx context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, fakeSQSRequest{queueURL: queueURL, entries: append([]sqsBatchEntry(nil), entries...)})
	p.mu.Unlock()

	<-ctx.Done()
	return sqsBatchResponse{}, ctx.Err()
}

func TestSendPubsubEventRespectsPublishTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 50 * time.Millisecond
	cfg.PublishResultGrace = 0
	a := &app{cfg: cfg, pubsub: blockingPubSubPublisher{}}

	evt := event{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "p"}}

	start := time.Now()
	err := a.sendPubsubEvent(context.Background(), evt, func(any) {})
	if err == nil {
		t.Fatal("expected a timeout error from a blocked publish")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("publish blocked for %s instead of timing out", elapsed)
	}
}

func TestSendSQS10EventsRespectsPublishTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 50 * time.Millisecond
	a := &app{cfg: cfg, sqs: blockingSQSPublisher{}}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "p"}},
	}

	start := time.Now()
	err := a.sendSQS10Events(context.Background(), "queue-a", events, func(any) {})
	if err == nil {
		t.Fatal("expected a timeout error from a blocked SendBatch")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SendBatch blocked for %s instead of timing out", elapsed)
	}
}

func TestValidateAWSWebIdentity(t *testing.T) {
	base := testConfig()
	base.AWSWebIdentityProvider = "google"
	base.AWSWebIdentityAudience = "//iam.example/aws"
	base.AWSRoleARN = "arn:aws:iam::123456789012:role/outboxer"

	if err := base.validate(); err != nil {
		t.Fatalf("expected a fully configured google web identity to be valid, got %v", err)
	}

	unsupported := base
	unsupported.AWSWebIdentityProvider = "azure"
	if err := unsupported.validate(); err == nil {
		t.Fatal("expected error for an unsupported web identity provider")
	}

	noRole := base
	noRole.AWSRoleARN = ""
	if err := noRole.validate(); err == nil {
		t.Fatal("expected error when web identity is set without a role ARN")
	}

	noAudience := base
	noAudience.AWSWebIdentityAudience = ""
	if err := noAudience.validate(); err == nil {
		t.Fatal("expected error when web identity is set without an audience")
	}

	off := testConfig()
	if err := off.validate(); err != nil {
		t.Fatalf("expected web identity to be optional, got %v", err)
	}
}

func TestValidateWatchdogMustExceedPollInterval(t *testing.T) {
	cfg := testConfig()
	cfg.PollInterval = time.Minute
	cfg.WatchdogInterval = 5 * time.Minute
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when watchdog interval is less than 10x the poll interval")
	}

	cfg.WatchdogInterval = 10 * time.Minute
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected watchdog interval of exactly 10x poll interval to be valid, got %v", err)
	}

	// A zero poll interval (the default hot loop) imposes no constraint.
	cfg.PollInterval = 0
	cfg.WatchdogInterval = time.Hour
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected zero poll interval to skip the watchdog check, got %v", err)
	}
}

func TestValidateWatchdogMustExceedBatchSendBound(t *testing.T) {
	cfg := testConfig()
	cfg.WatchdogInterval = cfg.batchSendBound()
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when watchdog interval does not exceed batch send bound")
	}

	cfg.WatchdogInterval = cfg.batchSendBound() + time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected watchdog interval over batch send bound to be valid, got %v", err)
	}
}

func TestValidateRequiresPositivePublishTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*appConfig)
	}{
		{"both backends", func(*appConfig) {}},
		{"pubsub only", func(cfg *appConfig) { cfg.SQSEnabled = false }},
		{"sqs only", func(cfg *appConfig) { cfg.PubSubEnabled = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.edit(&cfg)
			cfg.PublishTimeout = 0
			if err := cfg.validate(); err == nil {
				t.Fatal("expected error when publish timeout is zero")
			}

			cfg.PublishTimeout = -time.Millisecond
			if err := cfg.validate(); err == nil {
				t.Fatal("expected error when publish timeout is negative")
			}
		})
	}
}

func TestValidateRequiresNonNegativePublishResultGrace(t *testing.T) {
	cfg := testConfig()
	cfg.PublishResultGrace = -time.Millisecond
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when publish result grace is negative")
	}
}

func TestValidateRequiresPositiveOrderedGroupBatchCap(t *testing.T) {
	cfg := testConfig()
	cfg.OrderedGroupBatchCap = 0
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when ordered group batch cap is zero")
	}
}

func TestValidateRequiresPositiveSQSConcurrencyWhenSQSEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 0
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when SQS concurrency is zero and SQS is enabled")
	}

	cfg.SQSEnabled = false
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected SQS concurrency to be ignored when SQS is disabled, got %v", err)
	}
}

func TestBatchSendBound(t *testing.T) {
	cfg := testConfig()
	cfg.BatchSize = 32
	cfg.SQSSendConcurrency = 8
	cfg.OrderedGroupBatchCap = 8
	cfg.PublishTimeout = 30 * time.Second
	cfg.PublishResultGrace = 5 * time.Second

	if got, want := cfg.batchSendBound(), 8*35*time.Second; got != want {
		t.Fatalf("both backend bound = %s, want %s", got, want)
	}

	cfg.PubSubEnabled = false
	if got, want := cfg.batchSendBound(), 8*30*time.Second; got != want {
		t.Fatalf("SQS-only bound = %s, want %s", got, want)
	}

	cfg.OrderedGroupBatchCap = 1
	if got, want := cfg.batchSendBound(), 4*30*time.Second; got != want {
		t.Fatalf("SQS size-split bound = %s, want %s", got, want)
	}
	cfg.OrderedGroupBatchCap = 8

	cfg.PubSubEnabled = true
	cfg.SQSEnabled = false
	if got, want := cfg.batchSendBound(), 8*35*time.Second; got != want {
		t.Fatalf("Pub/Sub-only bound = %s, want %s", got, want)
	}
}

func TestProcessEventsStopsOnContextCancel(t *testing.T) {
	a := &app{cfg: testConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		a.processEvents(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processEvents did not return after context cancellation")
	}
}

func TestProcessEventsStopsAfterFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}}

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", "key-a", nil).
		AddRow("event-2", "pubsub", "topic-1", "two", "key-a", nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	done := make(chan struct{})
	go func() {
		a.processEvents(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processEvents did not stop after fatal-after-commit error")
	}
}

func TestProcessEventsDoesNotCooldownAfterNonFatalSenderError(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	cfg.ErrorCooldown = time.Hour
	expectedErr := errors.New("retryable pubsub")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, expectedErr}}

	firstRows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", nil, nil).
		AddRow("event-2", "pubsub", "topic-1", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(firstRows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	secondErr := errors.New("second select failed")
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnError(secondErr)
	mock.ExpectRollback()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.processEvents(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processEvents did not return after cancellation")
	}
}

func TestSleepContextReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	sleepContext(ctx, time.Hour)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("sleepContext blocked for %s on a cancelled context", elapsed)
	}
}

func TestDeadlockDetectorConcurrentAccess(_ *testing.T) {
	// Exercises the watchdog counter from two goroutines so the race detector
	// would flag a regression back to an unsynchronized int64.
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				deadlockDetector.Store(randomInt63())
			}
		}
	}()

	var previous int64
	for i := 0; i < 100000; i++ {
		previous = deadlockDetector.Load()
	}
	_ = previous

	close(stop)
	wg.Wait()
}

func TestFailureLoggerRateLimitsBySignature(t *testing.T) {
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	window := 2 * time.Minute
	logger := newFailureLogger(window)
	logger.now = func() time.Time { return now }

	if ok, suppressed := logger.shouldLog("destination-a|retryable"); !ok || suppressed != 0 {
		t.Fatalf("first occurrence = (%t, %d), want (true, 0)", ok, suppressed)
	}
	if ok, suppressed := logger.shouldLog("destination-a|retryable"); ok || suppressed != 0 {
		t.Fatalf("second occurrence = (%t, %d), want (false, 0)", ok, suppressed)
	}
	if ok, suppressed := logger.shouldLog("destination-a|permission"); !ok || suppressed != 0 {
		t.Fatalf("different signature = (%t, %d), want (true, 0)", ok, suppressed)
	}

	now = now.Add(window + time.Nanosecond)
	if ok, suppressed := logger.shouldLog("destination-a|retryable"); !ok || suppressed != 1 {
		t.Fatalf("post-window occurrence = (%t, %d), want (true, 1)", ok, suppressed)
	}
	if ok, suppressed := logger.shouldLog("destination-a|retryable"); ok || suppressed != 0 {
		t.Fatalf("post-summary repeat = (%t, %d), want (false, 0)", ok, suppressed)
	}
}

func TestFailureLoggerSkipsContextCancellationFallout(t *testing.T) {
	logger := newFailureLogger(time.Minute)
	a := &app{failureLogger: logger}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a.logFailure(ctx, "should be skipped", "signature")
	if len(logger.entries) != 0 {
		t.Fatalf("expected canceled failure log to be skipped, got %#v", logger.entries)
	}
}

func TestPubSubLocalPrevalidationBoundaries(t *testing.T) {
	attrs := map[string]string{}
	for i := 0; i < pubsubMaxAttributes; i++ {
		attrs[fmt.Sprintf("attr%d", i)] = "value"
	}
	if !validPubSubAttributes(attrs) {
		t.Fatal("expected exactly max Pub/Sub attributes to be valid")
	}
	attrs["overflow"] = "value"
	if validPubSubAttributes(attrs) {
		t.Fatal("expected too many Pub/Sub attributes to be invalid")
	}

	if !validPubSubAttributes(map[string]string{strings.Repeat("k", pubsubMaxAttributeKeyBytes): "value"}) {
		t.Fatal("expected max-length Pub/Sub attribute key to be valid")
	}
	if validPubSubAttributes(map[string]string{strings.Repeat("k", pubsubMaxAttributeKeyBytes+1): "value"}) {
		t.Fatal("expected overlong Pub/Sub attribute key to be invalid")
	}
	if !validPubSubAttributes(map[string]string{"key": strings.Repeat("v", pubsubMaxAttributeValueBytes)}) {
		t.Fatal("expected max-length Pub/Sub attribute value to be valid")
	}
	if validPubSubAttributes(map[string]string{"key": strings.Repeat("v", pubsubMaxAttributeValueBytes+1)}) {
		t.Fatal("expected overlong Pub/Sub attribute value to be invalid")
	}
	if validPubSubAttributes(map[string]string{"googclient": "value"}) {
		t.Fatal("expected goog-prefixed Pub/Sub attribute key to be invalid")
	}

	if reason, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1", Data: make([]byte, pubsubMaxMessageDataBytes)}); poison {
		t.Fatalf("expected exactly max Pub/Sub data to be accepted, got poison: %s", reason)
	}
	if _, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1", Data: make([]byte, pubsubMaxMessageDataBytes+1)}); !poison {
		t.Fatal("expected overlarge Pub/Sub data to be poison")
	}
	if _, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1", OrderingKey: "key-a"}); poison {
		t.Fatal("expected ordering-key-only Pub/Sub message not to be local poison")
	}
	if _, poison := pubsubPoisonReason(pubsubMessage{Topic: "topic-1"}); !poison {
		t.Fatal("expected empty Pub/Sub message with no attributes or key to be poison")
	}
}

func TestPubSubTopicSyntaxValidation(t *testing.T) {
	valid := []string{
		"abc",
		"topic-1",
		"projects/project-a/topics/topic-1",
		"projects/123/topics/a.b_c~d+e%f",
	}
	for _, topic := range valid {
		if !validPubSubTopic(topic) {
			t.Fatalf("expected Pub/Sub topic %q to be valid", topic)
		}
	}

	invalid := []string{
		"",
		"ab",
		"1topic",
		"googtopic",
		"bad/topic",
		"projects/project-a/topics/1bad",
		"projects//topics/topic-1",
		"projects/project-a/subscriptions/sub-1",
	}
	for _, topic := range invalid {
		if validPubSubTopic(topic) {
			t.Fatalf("expected Pub/Sub topic %q to be invalid", topic)
		}
	}
}

func TestSQSLocalPrevalidationBoundaries(t *testing.T) {
	attrs := map[string]string{}
	for i := 0; i < sqsMaxAttributes; i++ {
		attrs[fmt.Sprintf("attr%d", i)] = "value"
	}
	if !validSQSAttributes(attrs) {
		t.Fatal("expected exactly max SQS attributes to be valid")
	}
	attrs["overflow"] = "value"
	if validSQSAttributes(attrs) {
		t.Fatal("expected too many SQS attributes to be invalid")
	}

	invalidAttrs := []map[string]string{
		{"": "value"},
		{".bad": "value"},
		{"bad.": "value"},
		{"bad..name": "value"},
		{"AWS.trace": "value"},
		{"Amazon.trace": "value"},
		{"bad name": "value"},
		{strings.Repeat("k", 257): "value"},
		{"empty": ""},
	}
	for _, attr := range invalidAttrs {
		if validSQSAttributes(attr) {
			t.Fatalf("expected SQS attributes %#v to be invalid", attr)
		}
	}

	if isSQSPoison([]byte("body"), nil, false, "") {
		t.Fatal("expected ordinary SQS body to be valid")
	}
	if !isSQSPoison(nil, nil, false, "") {
		t.Fatal("expected empty SQS body to be poison")
	}
	if !isSQSPoison([]byte{0xff}, nil, false, "") {
		t.Fatal("expected invalid UTF-8 SQS body to be poison")
	}
	if !isSQSPoison([]byte("body"), map[string]string{"attr": string([]byte{0xff})}, false, "") {
		t.Fatal("expected invalid UTF-8 SQS attribute value to be poison")
	}
	if isSQSPoison([]byte("body\t\n\r"), nil, false, "") {
		t.Fatal("expected allowed SQS boundary characters to be valid")
	}
	if !isSQSPoison([]byte(strings.Repeat("x", sqsEventMaxSizeByte+1)), nil, false, "") {
		t.Fatal("expected oversized SQS message to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, true, strings.Repeat("x", 129)) {
		t.Fatal("expected overlong FIFO group id to be poison")
	}
	if !isSQSPoison([]byte("body"), nil, true, "bad\nkey") {
		t.Fatal("expected invalid FIFO group id to be poison")
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

	err := a.sendPubsubEvent(context.Background(), evt, func(id any) {
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

func TestCloudPubSubPublisherReusesCachedPublisherPerTopic(t *testing.T) {
	created := map[string]int{}
	publishers := map[string]*fakeTopicPublisher{}
	publisher := &cloudPubSubPublisher{
		cfg:        testConfig(),
		publishers: map[string]pubsubTopicPublisher{},
	}
	publisher.newPublisher = func(topic string) pubsubTopicPublisher {
		created[topic]++
		topicPublisher := &fakeTopicPublisher{}
		publishers[topic] = topicPublisher
		return topicPublisher
	}

	publisher.Flush("topic-1")
	publisher.ResumePublish("topic-1", "key-a")
	publisher.Flush("topic-1")
	publisher.Flush("topic-2")

	if !reflect.DeepEqual(created, map[string]int{"topic-1": 1, "topic-2": 1}) {
		t.Fatalf("unexpected publisher creation counts: %#v", created)
	}
	if publishers["topic-1"].flushes != 2 {
		t.Fatalf("expected cached topic-1 publisher to be flushed twice, got %d", publishers["topic-1"].flushes)
	}
	if !reflect.DeepEqual(publishers["topic-1"].resumes, []string{"key-a"}) {
		t.Fatalf("unexpected topic-1 resumes: %#v", publishers["topic-1"].resumes)
	}
}

func TestCloudPubSubPublisherCloseStopsCachedPublishers(t *testing.T) {
	publishers := map[string]*fakeTopicPublisher{}
	publisher := &cloudPubSubPublisher{
		cfg:        testConfig(),
		publishers: map[string]pubsubTopicPublisher{},
	}
	publisher.newPublisher = func(topic string) pubsubTopicPublisher {
		topicPublisher := &fakeTopicPublisher{}
		publishers[topic] = topicPublisher
		return topicPublisher
	}

	publisher.Flush("topic-1")
	publisher.Flush("topic-2")
	if err := publisher.Close(); err != nil {
		t.Fatalf("close publisher: %v", err)
	}

	for topic, topicPublisher := range publishers {
		if topicPublisher.stopCount != 1 {
			t.Fatalf("expected topic %s to be stopped once, got %d", topic, topicPublisher.stopCount)
		}
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

	err := a.sendPubsubEvent(context.Background(), evt, func(any) {})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected publisher error, got %v", err)
	}
}

func TestSendPubsubEventKeepsSyntacticallyValidMissingTopic(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("topic not found")
	a := &app{cfg: cfg, pubsub: &fakePubSubPublisher{err: expectedErr}}
	var deleted []any

	evt := event{columns: map[string]any{
		"id":          "event-1",
		"destination": "topic-1",
		"payload":     "payload",
	}}

	err := a.sendPubsubEvent(context.Background(), evt, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected publisher error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("expected missing topic event to be kept, got deleted ids %#v", deleted)
	}
}

func TestSendPubsubEventsFlushesUnorderedBatch(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two"}},
	}

	if err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected two published messages, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1"}) {
		t.Fatalf("expected one flush for topic-1, got %#v", pubsub.flushes)
	}
}

func TestSendPubsubEventsFlushesEachUnorderedTopic(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-2", "payload": "two"}},
	}

	if err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	sort.Strings(pubsub.flushes)
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1", "topic-2"}) {
		t.Fatalf("expected flush per unordered topic, got %#v", pubsub.flushes)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsOrderedKeySuccessIsSequentialAndCapped(t *testing.T) {
	cfg := testConfig()
	cfg.OrderedGroupBatchCap = 2
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "ordering_key": "key-a"}},
	}

	if err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected cap to publish two messages, got %#v", pubsub.messages)
	}
	for i, message := range pubsub.messages {
		if got, want := string(message.Data), []string{"one", "two"}[i]; got != want {
			t.Fatalf("message %d data = %q, want %q", i, got, want)
		}
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1", "topic-1"}) {
		t.Fatalf("expected per-message ordered flushes, got %#v", pubsub.flushes)
	}
}

func TestSendPubsubEventsOrderedKeyPreservesOrderAcrossBatches(t *testing.T) {
	cfg := testConfig()
	cfg.OrderedGroupBatchCap = 2
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}

	firstBatch := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "ordering_key": "key-a"}},
	}
	if err := a.sendPubsubEvents(context.Background(), firstBatch, func(any) {}); err != nil {
		t.Fatalf("first sendPubsubEvents returned error: %v", err)
	}

	secondBatch := []event{
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-4", "destination": "topic-1", "payload": "four", "ordering_key": "key-a"}},
	}
	if err := a.sendPubsubEvents(context.Background(), secondBatch, func(any) {}); err != nil {
		t.Fatalf("second sendPubsubEvents returned error: %v", err)
	}

	got := []string{}
	for _, message := range pubsub.messages {
		got = append(got, string(message.Data))
	}
	if !reflect.DeepEqual(got, []string{"one", "two", "three", "four"}) {
		t.Fatalf("unexpected ordered publish sequence: %#v", got)
	}
}

func TestSendPubsubEventsOrderedKeysProgressConcurrently(t *testing.T) {
	cfg := testConfig()
	pubsub := &concurrentPubSubPublisher{
		started: make(chan string, 2),
		release: make(chan struct{}, 2),
	}
	a := &app{cfg: cfg, pubsub: pubsub}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-b"}},
	}

	done := make(chan error, 1)
	go func() {
		done <- a.sendPubsubEvents(context.Background(), events, func(any) {})
	}()

	started := []string{}
	for i := 0; i < 2; i++ {
		select {
		case key := <-pubsub.started:
			started = append(started, key)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ordered keys to start")
		}
	}
	sort.Strings(started)
	if !reflect.DeepEqual(started, []string{"key-a", "key-b"}) {
		t.Fatalf("expected both ordered keys to wait concurrently, got %#v", started)
	}

	pubsub.release <- struct{}{}
	pubsub.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}
}

func TestSendPubsubEventsMixedOrderedAndUnorderedSuccess(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "ordered-1", "destination": "topic-1", "payload": "ordered", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "unordered-1", "destination": "topic-1", "payload": "unordered"}},
	}

	if err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}
	sort.Slice(deleted, func(i, j int) bool { return fmt.Sprint(deleted[i]) < fmt.Sprint(deleted[j]) })
	if !reflect.DeepEqual(deleted, []any{"ordered-1", "unordered-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected two Pub/Sub messages, got %#v", pubsub.messages)
	}
}

func TestSendPubsubEventsUnorderedUnknownResultIsKept(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	cfg.PublishResultGrace = 0
	pubsub := &fakePubSubPublisher{results: []fakePubSubResult{{block: true}, {}}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsWaitsThroughPublishResultGrace(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	cfg.PublishResultGrace = 80 * time.Millisecond
	pubsub := &fakePubSubPublisher{results: []fakePubSubResult{{delay: 50 * time.Millisecond}}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
	}

	if err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsCanceledResultIsKept(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = time.Hour
	pubsub := &fakePubSubPublisher{results: []fakePubSubResult{{block: true}}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one"}},
	}

	err := a.sendPubsubEvents(ctx, events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendPubsubEventsDoesNotPoisonMultiEventPublishLimits(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	largeEvents := []event{
		{columns: map[string]any{"id": "large-1", "destination": "topic-1", "payload": strings.Repeat("a", 6_000_000)}},
		{columns: map[string]any{"id": "large-2", "destination": "topic-1", "payload": strings.Repeat("b", 6_000_000)}},
	}
	if err := a.sendPubsubEvents(context.Background(), largeEvents, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents for large events returned error: %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"large-1", "large-2"}) {
		t.Fatalf("unexpected deleted ids for large events: %#v", deleted)
	}

	manyEvents := make([]event, pubsubMaxPublishRequestMessages+1)
	for i := range manyEvents {
		manyEvents[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("many-%04d", i),
			"destination": "topic-1",
			"payload":     "payload",
		}}
	}
	deleted = nil
	if err := a.sendPubsubEvents(context.Background(), manyEvents, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents for many events returned error: %v", err)
	}
	if len(deleted) != len(manyEvents) {
		t.Fatalf("expected all many events deleted, got %d of %d", len(deleted), len(manyEvents))
	}
}

func TestSendPubsubEventsDropsLocalPoisonWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	tooManyAttributes := map[string]any{}
	for i := 0; i < pubsubMaxAttributes+1; i++ {
		tooManyAttributes[fmt.Sprintf("attr%d", i)] = "value"
	}
	events := []event{
		{columns: map[string]any{"id": "empty", "destination": "topic-1", "payload": ""}},
		{columns: map[string]any{"id": "large", "destination": "topic-1", "payload": strings.Repeat("x", pubsubMaxMessageDataBytes+1)}},
		{columns: map[string]any{"id": "attrs", "destination": "topic-1", "payload": "body", "attributes": tooManyAttributes}},
		{columns: map[string]any{"id": "topic", "destination": "1-bad-topic", "payload": "body"}},
	}

	if err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"empty", "large", "attrs", "topic"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no provider calls for local poison, got %#v", pubsub.messages)
	}
}

func TestSendPubsubEventsIsolatesPermanentUnorderedFailure(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{errs: []error{pubsubPermanentError("bundle"), nil}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "payload"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected initial publish plus isolated retry, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.flushes, []string{"topic-1", "topic-1"}) {
		t.Fatalf("expected flush for initial publish and isolation, got %#v", pubsub.flushes)
	}
}

func TestSendPubsubEventsIsolatesPermanentBadEventAndValidEvent(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{errs: []error{
		pubsubPermanentError("bundle"),
		pubsubPermanentError("bundle"),
		pubsubPermanentError("bad event"),
		nil,
	}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "bad", "destination": "topic-1", "payload": "bad"}},
		{columns: map[string]any{"id": "valid", "destination": "topic-1", "payload": "valid"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendPubsubEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"bad", "valid"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 4 {
		t.Fatalf("expected two bundled publishes plus two isolated publishes, got %#v", pubsub.messages)
	}
}

func TestSendPubsubEventsOrderedRetryableFailureResumesAndStopsKey(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("retryable")
	pubsub := &fakePubSubPublisher{err: expectedErr}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-a"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected retryable error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 1 {
		t.Fatalf("expected only first key event to be published, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.resumes, []fakePubSubResume{{topic: "topic-1", orderingKey: "key-a"}}) {
		t.Fatalf("unexpected resumes: %#v", pubsub.resumes)
	}
}

func TestSendPubsubEventsOrderedFailureAfterSuccessKeepsRemainder(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("retryable")
	pubsub := &fakePubSubPublisher{errs: []error{nil, expectedErr}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "topic-1", "payload": "three", "ordering_key": "key-a"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected retryable error, got %v", err)
	}
	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected only first two key events to be published, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.resumes, []fakePubSubResume{{topic: "topic-1", orderingKey: "key-a"}}) {
		t.Fatalf("unexpected resumes: %#v", pubsub.resumes)
	}
}

func TestSendPubsubEventsOrderedIsolationStopsAtFirstNonDone(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("still retryable")
	pubsub := &fakePubSubPublisher{errs: []error{pubsubPermanentError("bundle"), expectedErr}}
	a := &app{cfg: cfg, pubsub: pubsub}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-a"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected isolated retryable error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(pubsub.messages) != 2 {
		t.Fatalf("expected initial publish plus isolated retry, got %#v", pubsub.messages)
	}
	if !reflect.DeepEqual(pubsub.resumes, []fakePubSubResume{
		{topic: "topic-1", orderingKey: "key-a"},
		{topic: "topic-1", orderingKey: "key-a"},
	}) {
		t.Fatalf("unexpected resumes: %#v", pubsub.resumes)
	}
}

func TestSendPubsubEventsOrderedUnknownResultIsFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	cfg.PublishResultGrace = 0
	a := &app{cfg: cfg, pubsub: blockingPubSubPublisher{}}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "topic-1", "payload": "one", "ordering_key": "key-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "topic-1", "payload": "two", "ordering_key": "key-a"}},
	}

	err := a.sendPubsubEvents(context.Background(), events, func(any) {})
	if !errors.Is(err, errFatalAfterCommit) {
		t.Fatalf("expected fatal-after-commit error, got %v", err)
	}
}

func TestSendSQS10EventsHandlesStandardPartialResponses(t *testing.T) {
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
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "attributes": []byte(`{"ok":"1","bad":true}`)}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": "two"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a", "payload": "three"}},
	}

	err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
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
	if !reflect.DeepEqual(sqs.requests[0].entries[0].Attributes, map[string]string{"ok": "1"}) {
		t.Fatalf("unexpected sanitized attributes: %#v", sqs.requests[0].entries[0].Attributes)
	}
}

func TestSendSQS10EventsIsolatesPermanentBatchRequestError(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{
		errs: []error{fakeSQSAPIError{code: "InvalidMessageContents"}, nil, nil},
		responses: []sqsBatchResponse{
			{Successful: []sqsBatchSuccess{{ID: "event-1", MessageID: "message-1"}}},
			{Successful: []sqsBatchSuccess{{ID: "event-2", MessageID: "message-2"}}},
		},
	}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": "two"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 3 {
		t.Fatalf("expected original batch plus two isolated sends, got %#v", sqs.requests)
	}
	if len(sqs.requests[0].entries) != 2 || len(sqs.requests[1].entries) != 1 || len(sqs.requests[2].entries) != 1 {
		t.Fatalf("unexpected isolation request shapes: %#v", sqs.requests)
	}
}

func TestSendSQS10EventsRetryableRequestErrorKeepsEvents(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("temporary SQS outage")
	sqs := &fakeSQSPublisher{err: expectedErr}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one"}},
	}

	err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected retryable SQS error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQS10EventsCanceledContextKeepsEvents(t *testing.T) {
	cfg := testConfig()
	sqs := &recordingBlockingSQSPublisher{}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "payload"}},
	}

	err := a.sendSQS10Events(ctx, "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQSEventsStandardUsesConcurrencyLimit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 2
	sqs := &trackingSQSPublisher{
		started: make(chan struct{}, 3),
		release: make(chan struct{}, 3),
	}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 25)
	for i := range events {
		events[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("event-%02d", i),
			"destination": "queue-a",
			"payload":     "payload",
		}}
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	done := make(chan error, 1)
	go func() {
		done <- a.sendSQSEvents(context.Background(), events, func(id any) {
			deletedMu.Lock()
			defer deletedMu.Unlock()
			deleted = append(deleted, id)
		})
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-sqs.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial SQS requests")
		}
	}
	select {
	case <-sqs.started:
		t.Fatal("third SQS request started before concurrency slot was released")
	case <-time.After(50 * time.Millisecond):
	}

	sqs.release <- struct{}{}
	sqs.release <- struct{}{}
	select {
	case <-sqs.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for final SQS request")
	}
	sqs.release <- struct{}{}

	if err := <-done; err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	sqs.mu.Lock()
	maxInFlight := sqs.max
	requestCount := len(sqs.requests)
	sqs.mu.Unlock()
	if maxInFlight > 2 {
		t.Fatalf("expected at most 2 concurrent SQS requests, got %d", maxInFlight)
	}
	if requestCount != 3 {
		t.Fatalf("expected 3 chunked SQS requests, got %d", requestCount)
	}
	deletedMu.Lock()
	deletedCount := len(deleted)
	deletedMu.Unlock()
	if deletedCount != len(events) {
		t.Fatalf("expected all events deleted, got %d", deletedCount)
	}
}

func TestSendSQSEventsStandardSplitsByBatchSize(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": strings.Repeat("a", 600*1024)}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a", "payload": strings.Repeat("b", 600*1024)}},
	}

	var deletedMu sync.Mutex
	deleted := []any{}
	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 2 {
		t.Fatalf("expected size split into two SQS requests, got %#v", sqs.requests)
	}
	for _, request := range sqs.requests {
		if len(request.entries) != 1 {
			t.Fatalf("expected one entry per size-split request, got %#v", request.entries)
		}
	}
	if len(deleted) != 2 {
		t.Fatalf("expected both events deleted, got %#v", deleted)
	}
}

func TestSendSQSEventsStandardSplitsByCount(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 11)
	for i := range events {
		events[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("event-%02d", i),
			"destination": "queue-a",
			"payload":     "payload",
		}}
	}

	if err := a.sendSQSEvents(context.Background(), events, func(any) {}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if len(sqs.requests) != 2 {
		t.Fatalf("expected 11 events to split into two requests, got %#v", sqs.requests)
	}
	sizes := []int{len(sqs.requests[0].entries), len(sqs.requests[1].entries)}
	sort.Ints(sizes)
	if !reflect.DeepEqual(sizes, []int{1, 10}) {
		t.Fatalf("unexpected chunk sizes: %#v", sizes)
	}
}

func TestSendSQSEventsStandardSendsTenInOneBatch(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := make([]event, 10)
	for i := range events {
		events[i] = event{columns: map[string]any{
			"id":          fmt.Sprintf("event-%02d", i),
			"destination": "queue-a",
			"payload":     "payload",
		}}
	}

	if err := a.sendSQSEvents(context.Background(), events, func(any) {}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected one SQS request, got %#v", sqs.requests)
	}
	if len(sqs.requests[0].entries) != 10 {
		t.Fatalf("expected ten SQS entries, got %#v", sqs.requests[0].entries)
	}
}

func TestSendSQSEventsFIFOStopsGroupAfterRetryableFailure(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{responses: []sqsBatchResponse{
		{Successful: []sqsBatchSuccess{{ID: "event-1", MessageID: "message-1"}}},
		{Failed: []sqsBatchFailure{{ID: "event-2", Code: "InternalError", Message: "retry", SenderFault: false}}},
	}}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "ordering_key": "group-a"}},
	}

	err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 2 {
		t.Fatalf("expected two single-message FIFO requests, got %#v", sqs.requests)
	}
	for _, request := range sqs.requests {
		if len(request.entries) != 1 {
			t.Fatalf("expected one FIFO entry per request, got %#v", request.entries)
		}
		entry := request.entries[0]
		if entry.MessageGroupID != "group-a" {
			t.Fatalf("expected FIFO group id group-a, got %q", entry.MessageGroupID)
		}
		if entry.DeduplicationID != entry.ID {
			t.Fatalf("expected raw valid event id as dedup id, got %q for %q", entry.DeduplicationID, entry.ID)
		}
	}
}

func TestSendSQSEventsFIFOOneGroupAllSuccessUsesSingleMessageRequests(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "ordering_key": "group-a"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2", "event-3"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 3 {
		t.Fatalf("expected three single-message FIFO requests, got %#v", sqs.requests)
	}
	for _, request := range sqs.requests {
		if len(request.entries) != 1 {
			t.Fatalf("expected one entry per FIFO request, got %#v", request.entries)
		}
		if request.entries[0].MessageGroupID != "group-a" {
			t.Fatalf("unexpected message group ID: %q", request.entries[0].MessageGroupID)
		}
	}
}

func TestSendSQSEventsFIFOProcessesDifferentGroups(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deletedMu sync.Mutex
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-b"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	deletedMu.Lock()
	deletedCopy := append([]any(nil), deleted...)
	deletedMu.Unlock()
	sort.Slice(deletedCopy, func(i, j int) bool { return fmt.Sprint(deletedCopy[i]) < fmt.Sprint(deletedCopy[j]) })
	if !reflect.DeepEqual(deletedCopy, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deletedCopy)
	}
	if len(sqs.requests) != 2 {
		t.Fatalf("expected one request per FIFO group event, got %#v", sqs.requests)
	}
	groups := []string{sqs.requests[0].entries[0].MessageGroupID, sqs.requests[1].entries[0].MessageGroupID}
	sort.Strings(groups)
	if !reflect.DeepEqual(groups, []string{"group-a", "group-b"}) {
		t.Fatalf("unexpected FIFO groups: %#v", groups)
	}
}

func TestSendSQSEventsFIFOAppliesGroupBatchCap(t *testing.T) {
	cfg := testConfig()
	cfg.OrderedGroupBatchCap = 2
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-3", "destination": "queue-a.fifo", "payload": "three", "ordering_key": "group-a"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 2 {
		t.Fatalf("expected cap to send two requests, got %#v", sqs.requests)
	}
}

func TestSendSQSEventsFIFODifferentGroupCanSucceedWhenOneFails(t *testing.T) {
	cfg := testConfig()
	sqs := &keyedSQSPublisher{responses: map[string]sqsBatchResponse{
		"event-1": {Failed: []sqsBatchFailure{{ID: "event-1", Code: "InternalError", Message: "retry", SenderFault: false}}},
		"event-2": {Successful: []sqsBatchSuccess{{ID: "event-2", MessageID: "message-2"}}},
	}}
	a := &app{cfg: cfg, sqs: sqs}
	var deletedMu sync.Mutex
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-b"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	deletedMu.Lock()
	defer deletedMu.Unlock()
	if !reflect.DeepEqual(deleted, []any{"event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
}

func TestSendSQSEventsFIFOContinuesAfterContentPoison(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
	}

	if err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQSEvents returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1", "event-2"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected only the non-poison event to reach SQS, got %#v", sqs.requests)
	}
	if got := sqs.requests[0].entries[0].ID; got != "event-2" {
		t.Fatalf("expected event-2 to be sent after poison event, got %q", got)
	}
}

func TestSendSQSEventsFIFOTimeoutStopsSameGroup(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 20 * time.Millisecond
	sqs := &recordingBlockingSQSPublisher{}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
		{columns: map[string]any{"id": "event-2", "destination": "queue-a.fifo", "payload": "two", "ordering_key": "group-a"}},
	}

	err := a.sendSQSEvents(context.Background(), events, func(id any) {
		deleted = append(deleted, id)
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	sqs.mu.Lock()
	requests := append([]fakeSQSRequest(nil), sqs.requests...)
	sqs.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("expected only first same-group event to be sent, got %#v", requests)
	}
	if got := requests[0].entries[0].ID; got != "event-1" {
		t.Fatalf("expected event-1 to be the only attempted send, got %q", got)
	}
}

func TestSendSQS10EventsStandardQueueOmitsFIFOFields(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	// A standard queue must not get a group id even when an ordering key is set.
	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a", "payload": "one", "ordering_key": "group-a"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.MessageGroupID != "" {
		t.Fatalf("expected no message group id on standard queue, got %q", entry.MessageGroupID)
	}
	if entry.DeduplicationID != "" {
		t.Fatalf("expected no dedup id on standard queue, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsFIFOWithoutOrderingKeyGetsFallbackGroup(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "queue-a.fifo", "payload": "one"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.MessageGroupID == "" {
		t.Fatal("expected a fallback message group id on FIFO queue")
	}
	if entry.MessageGroupID != syntheticFIFOGroupID("event-1") {
		t.Fatalf("expected stable synthetic group id, got %q", entry.MessageGroupID)
	}
	if entry.DeduplicationID != "event-1" {
		t.Fatalf("expected dedup id to equal event id, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsFIFODerivesSafeDedupID(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	rawID := strings.Repeat("x", 129)

	events := []event{
		{columns: map[string]any{"id": rawID, "destination": "queue-a.fifo", "payload": "one", "ordering_key": "group-a"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a.fifo", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.ID == rawID {
		t.Fatal("expected oversized raw event id to be replaced for batch entry id")
	}
	if entry.DeduplicationID == rawID {
		t.Fatal("expected oversized raw event id to be replaced for dedup id")
	}
	if len(entry.DeduplicationID) != 64 {
		t.Fatalf("expected SHA-256 hex dedup id, got %q", entry.DeduplicationID)
	}
}

func TestSendSQS10EventsStandardDerivesSafeBatchEntryID(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	rawID := strings.Repeat("x", 81)

	events := []event{
		{columns: map[string]any{"id": rawID, "destination": "queue-a", "payload": "one"}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(any) {}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	entry := sqs.requests[0].entries[0]
	if entry.ID == rawID {
		t.Fatal("expected oversized raw event id to be replaced for standard batch entry id")
	}
	if len(entry.ID) != 64 {
		t.Fatalf("expected SHA-256 hex entry id, got %q", entry.ID)
	}
}

func TestSendSQS10EventsDropsLocalPoisonWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "empty", "destination": "queue-a", "payload": ""}},
		{columns: map[string]any{"id": "large", "destination": "queue-a", "payload": strings.Repeat("x", sqsEventMaxSizeByte+1)}},
	}

	if err := a.sendSQS10Events(context.Background(), "queue-a", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"empty", "large"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no provider calls for local poison, got %#v", sqs.requests)
	}
}

func TestSendSQS10EventsDropsInvalidQueueURLWithoutProviderCall(t *testing.T) {
	cfg := testConfig()
	sqs := &fakeSQSPublisher{autoReply: true}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "bad queue url", "payload": "payload"}},
	}

	if err := a.sendSQS10Events(context.Background(), "bad queue url", events, func(id any) {
		deleted = append(deleted, id)
	}); err != nil {
		t.Fatalf("sendSQS10Events returned error: %v", err)
	}

	if !reflect.DeepEqual(deleted, []any{"event-1"}) {
		t.Fatalf("unexpected deleted ids: %#v", deleted)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no provider calls for invalid queue URL, got %#v", sqs.requests)
	}
}

func TestSendSQS10EventsKeepsSyntacticallyValidMissingQueue(t *testing.T) {
	cfg := testConfig()
	expectedErr := fakeSQSAPIError{code: "QueueDoesNotExist"}
	sqs := &fakeSQSPublisher{err: expectedErr}
	a := &app{cfg: cfg, sqs: sqs}
	var deleted []any

	events := []event{
		{columns: map[string]any{"id": "event-1", "destination": "https://sqs.us-east-1.amazonaws.com/123456789012/missing", "payload": "payload"}},
	}

	err := a.sendSQS10Events(context.Background(), "https://sqs.us-east-1.amazonaws.com/123456789012/missing", events, func(id any) {
		deleted = append(deleted, id)
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected missing queue error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("expected missing queue event to be kept, got deleted ids %#v", deleted)
	}
	if len(sqs.requests) != 1 {
		t.Fatalf("expected provider call for syntactically valid queue URL, got %#v", sqs.requests)
	}
}

func TestSendSQS10EventsKeepsRetryableProviderErrors(t *testing.T) {
	for _, code := range []string{"QueueDoesNotExist", "AccessDenied", "ExpiredToken", "ThrottlingException"} {
		t.Run(code, func(t *testing.T) {
			cfg := testConfig()
			expectedErr := fakeSQSAPIError{code: code}
			sqs := &fakeSQSPublisher{err: expectedErr}
			a := &app{cfg: cfg, sqs: sqs}
			var deleted []any

			events := []event{
				{columns: map[string]any{"id": "event-1", "destination": "https://sqs.us-east-1.amazonaws.com/123456789012/queue", "payload": "payload"}},
			}

			err := a.sendSQS10Events(context.Background(), "https://sqs.us-east-1.amazonaws.com/123456789012/queue", events, func(id any) {
				deleted = append(deleted, id)
			})
			if !errors.Is(err, expectedErr) {
				t.Fatalf("expected provider error, got %v", err)
			}
			if len(deleted) != 0 {
				t.Fatalf("expected event to be kept for %s, got deleted ids %#v", code, deleted)
			}
		})
	}
}

func TestProcessOneBatchCommitsDoneBeforeNonFatalSenderError(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("retryable pubsub")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, expectedErr}}

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", nil, nil).
		AddRow("event-2", "pubsub", "topic-1", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected non-fatal sender error, got %v", err)
	}
	if errors.Is(err, errDatabaseBatch) {
		t.Fatalf("sender error should not be classified as database error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
}

func TestProcessOneBatchBeginFailureIsDatabaseError(t *testing.T) {
	cfg := testConfig()
	expectedErr := errors.New("begin failed")
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub
	mock.ExpectBegin().WillReturnError(expectedErr)

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database begin error, got %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected no selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no sender calls after begin failure, got %#v", pubsub.messages)
	}
}

func TestProcessOneBatchEmptyBatchCommitsWithoutDelete(t *testing.T) {
	cfg := testConfig()
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{}
	a.sqs = &fakeSQSPublisher{autoReply: true}

	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(mockEventRows())
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected empty batch, got %d selected events", result.selected)
	}
}

func TestProcessOneBatchDeletesContentPoisonAndConfirmedSendTogether(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("poison", "sqs", "queue-a", "", nil, nil).
		AddRow("confirmed", "sqs", "queue-a", "payload", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteTwoSQL).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
	if len(sqs.requests) != 1 || len(sqs.requests[0].entries) != 1 || sqs.requests[0].entries[0].ID != "confirmed" {
		t.Fatalf("expected only confirmed event to be sent, got %#v", sqs.requests)
	}
}

func TestProcessOneBatchCommitsDoneBeforeFatalAfterCommit(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{errs: []error{nil, context.DeadlineExceeded}}

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", "key-a", nil).
		AddRow("event-2", "pubsub", "topic-1", "two", "key-a", nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, errFatalAfterCommit) {
		t.Fatalf("expected fatal-after-commit error, got %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
}

func TestProcessOneBatchRollsBackOnDeleteFailure(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("delete failed")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{}

	rows := mockEventRows().AddRow("event-1", "pubsub", "topic-1", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnError(expectedErr)
	mock.ExpectRollback()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database delete error, got %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchRollsBackOnSelectFailure(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("select failed")
	pubsub := &fakePubSubPublisher{}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub

	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnError(expectedErr)
	mock.ExpectRollback()

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database select error, got %v", err)
	}
	if result.selected != 0 {
		t.Fatalf("expected no selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no sender calls after select failure, got %#v", pubsub.messages)
	}
}

func TestProcessOneBatchCommitFailureIsDatabaseError(t *testing.T) {
	cfg := testConfig()
	cfg.SQSEnabled = false
	expectedErr := errors.New("commit failed")
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = &fakePubSubPublisher{}

	rows := mockEventRows().AddRow("event-1", "pubsub", "topic-1", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(expectedErr)

	result, err := a.processOneBatch(context.Background())
	if !errors.Is(err, expectedErr) || !errors.Is(err, errDatabaseBatch) {
		t.Fatalf("expected database commit error, got %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchRoutingFailuresOnlyCommitWithoutSendOrDelete(t *testing.T) {
	cfg := testConfig()
	pubsub := &fakePubSubPublisher{}
	sqs := &fakeSQSPublisher{autoReply: true}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("event-1", "kafka", "topic-1", "one", nil, nil).
		AddRow("event-2", "", "topic-2", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 2 {
		t.Fatalf("expected two selected events, got %d", result.selected)
	}
	if len(pubsub.messages) != 0 {
		t.Fatalf("expected no Pub/Sub sends for routing failures, got %#v", pubsub.messages)
	}
	if len(sqs.requests) != 0 {
		t.Fatalf("expected no SQS sends for routing failures, got %#v", sqs.requests)
	}
}

func TestProcessOneBatchDeduplicatesDoneIDs(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{response: sqsBatchResponse{
		Successful: []sqsBatchSuccess{
			{ID: "event-1", MessageID: "message-1"},
			{ID: "event-1", MessageID: "message-1-duplicate"},
		},
	}}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().AddRow("event-1", "sqs", "queue-a", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteOneSQL).WithArgs("event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchIgnoresDoneIDOutsideSelectedBatch(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	sqs := &fakeSQSPublisher{response: sqsBatchResponse{
		Successful: []sqsBatchSuccess{{ID: "unknown-entry", MessageID: "message-unknown"}},
	}}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.sqs = sqs

	rows := mockEventRows().AddRow("event-1", "sqs", "queue-a", "one", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := a.processOneBatch(context.Background())
	if err != nil {
		t.Fatalf("processOneBatch returned error: %v", err)
	}
	if result.selected != 1 {
		t.Fatalf("expected one selected event, got %d", result.selected)
	}
}

func TestProcessOneBatchRunsEnabledBackendsConcurrently(t *testing.T) {
	cfg := testConfig()
	pubsub := &trackingPubSubPublisher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	sqs := &trackingSQSPublisher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	a, mock, cleanup := newMockProcessorApp(t, cfg)
	defer cleanup()
	a.pubsub = pubsub
	a.sqs = sqs

	rows := mockEventRows().
		AddRow("event-1", "pubsub", "topic-1", "one", nil, nil).
		AddRow("event-2", "sqs", "queue-a", "two", nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery(selectEventsSQL).WithArgs(cfg.BatchSize).WillReturnRows(rows)
	mock.ExpectExec(deleteTwoSQL).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	done := make(chan error, 1)
	go func() {
		_, err := a.processOneBatch(context.Background())
		done <- err
	}()

	for name, started := range map[string]chan struct{}{
		"pubsub": pubsub.started,
		"sqs":    sqs.started,
	} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("%s backend did not start before release", name)
		}
	}

	pubsub.release <- struct{}{}
	sqs.release <- struct{}{}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processOneBatch returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("processOneBatch did not finish after releasing backends")
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
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	}()

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
