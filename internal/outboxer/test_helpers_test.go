package outboxer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql/driver"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	outboxpubsub "github.com/fvdsn/outboxer/internal/outboxer/pubsub"
	outboxsqs "github.com/fvdsn/outboxer/internal/outboxer/sqs"
)

type pubsubPublishResult = outboxpubsub.PublishResult
type pubsubMessage = outboxpubsub.Message
type sqsBatchEntry = outboxsqs.BatchEntry
type sqsBatchResponse = outboxsqs.BatchResponse
type sqsBatchSuccess = outboxsqs.BatchSuccess

type fakePubSubPublisher struct {
	mu       sync.Mutex
	err      error
	errs     []error
	results  []fakePubSubResult
	messages []pubsubMessage
	flushes  []string
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

func (p *fakePubSubPublisher) ResumePublish(string, string) {}

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

func testConfig() appConfig {
	return appConfig{
		EventTable:       "events",
		EventID:          "id",
		EventTimestamp:   "timestamp",
		EventPayload:     "payload",
		EventTarget:      "target",
		EventDestination: "destination",
		EventOptions:     "options",

		CollectBatchTarget: 5000,
		SQSSendConcurrency: 8,
		NotifyChannel:      "outboxer_events",

		WatchdogInterval:   time.Hour,
		HealthPort:         9999,
		PubSubEnabled:      true,
		SQSEnabled:         true,
		DefaultPubSubTopic: "default",
		ErrorCooldown:      time.Millisecond,
		PublishTimeout:     30 * time.Second,
		PublishResultGrace: 5 * time.Second,
		MaxEventAge:        0,
		StatsInterval:      10 * time.Second,

		PGSchema: "public",
	}
}

const (
	deleteOneSQL = `DELETE FROM "public"."events" WHERE "id" IN ($1)`
	deleteTwoSQL = `DELETE FROM "public"."events" WHERE "id" IN ($1, $2)`
)

func expectSelectEvents(mock sqlmock.Sqlmock, a *app) *sqlmock.ExpectedQuery {
	query, args := a.selectEventsQuery()
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	return mock.ExpectQuery(query).WithArgs(values...)
}

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
	return &app{cfg: cfg, db: db, failureLogger: newFailureLogger(time.Minute), stats: &appStats{}}, mock, cleanup
}

func mockEventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"resolved_target", "resolved_destination", "id", "target", "destination", "payload", "options"})
}

func mockEventRowsWithTimestamp() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"resolved_target", "resolved_destination", "id", "target", "destination", "payload", "options", "timestamp"})
}

func mockEventRow(id driver.Value, target string, destination string, payload driver.Value, options driver.Value, extra ...driver.Value) []driver.Value {
	values := []driver.Value{target, destination, id, target, destination, payload, options}
	return append(values, extra...)
}

func testEvent(id, target, destination, payload string) event {
	columns := map[string]any{
		"id":      id,
		"payload": payload,
	}
	if target != "" {
		columns["target"] = target
	}
	if destination != "" {
		columns["destination"] = destination
	}
	return event{columns: columns}
}

func combinedOrderingOptions() map[string]any {
	return map[string]any{
		"pubsub": map[string]any{"orderingKey": "key-a"},
		"sqs":    map[string]any{"messageGroupId": "key-a"},
	}
}

func mockRowsForEvents(cfg appConfig, events []event) *sqlmock.Rows {
	rows := mockEventRows()
	for _, evt := range events {
		target, destination := resolvedTestRoute(cfg, evt)
		rows.AddRow(
			target,
			destination,
			eventValue(evt, "id"),
			eventString(evt, "target"),
			eventString(evt, "destination"),
			eventValue(evt, "payload"),
			mockDBValue(eventValue(evt, "options")),
		)
	}
	return rows
}

func resolvedTestRoute(cfg appConfig, evt event) (string, string) {
	target := eventString(evt, "target")
	if target == "" {
		switch {
		case cfg.PubSubEnabled && !cfg.SQSEnabled:
			target = eventTargetPubSub
		case cfg.SQSEnabled && !cfg.PubSubEnabled:
			target = eventTargetSQS
		}
	}

	destination := eventString(evt, "destination")
	if destination == "" {
		switch target {
		case eventTargetPubSub:
			destination = cfg.DefaultPubSubTopic
		case eventTargetSQS:
			destination = cfg.DefaultSQSQueueURL
		}
	}
	return target, destination
}

func mockDBValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return nil
		}
		return encoded
	default:
		return value
	}
}

func deleteEventsSQL(count int) string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf(`DELETE FROM "public"."events" WHERE "id" IN (%s)`, strings.Join(placeholders, ", "))
}

func anySQLArgs(count int) []driver.Value {
	args := make([]driver.Value, count)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}

func assertStatsSnapshot(t *testing.T, got statsSnapshot, want statsSnapshot) {
	t.Helper()
	if got != want {
		t.Fatalf("unexpected stats snapshot:\ngot  %#v\nwant %#v", got, want)
	}
}

func sqsEntryCountByQueue(requests []fakeSQSRequest) map[string]int {
	counts := map[string]int{}
	for _, request := range requests {
		counts[request.queueURL] += len(request.entries)
	}
	return counts
}

func pubsubMessageCountByTopic(messages []pubsubMessage) map[string]int {
	counts := map[string]int{}
	for _, message := range messages {
		counts[message.Topic]++
	}
	return counts
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

func strconvNano() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
