package outboxer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql/driver"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
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

		CollectBatchTarget:   5000,
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
	deleteOneSQL = `DELETE FROM "events" WHERE "id" IN ($1)`
	deleteTwoSQL = `DELETE FROM "events" WHERE "id" IN ($1, $2)`
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
	return &app{cfg: cfg, db: db, failureLogger: newFailureLogger(time.Minute)}, mock, cleanup
}

func mockEventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "target", "destination", "payload", "ordering_key", "attributes"})
}

func testEvent(id, target, destination, payload, orderingKey string) event {
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
	if orderingKey != "" {
		columns["ordering_key"] = orderingKey
	}
	return event{columns: columns}
}

func mockRowsForEvents(events []event) *sqlmock.Rows {
	rows := mockEventRows()
	for _, evt := range events {
		rows.AddRow(
			eventValue(evt, "id"),
			eventValue(evt, "target"),
			eventValue(evt, "destination"),
			eventValue(evt, "payload"),
			eventValue(evt, "ordering_key"),
			eventValue(evt, "attributes"),
		)
	}
	return rows
}

func deleteEventsSQL(count int) string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf(`DELETE FROM "events" WHERE "id" IN (%s)`, strings.Join(placeholders, ", "))
}

func anySQLArgs(count int) []driver.Value {
	args := make([]driver.Value, count)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}

func sortedDeletedIDs(deleted []any) []string {
	ids := make([]string, 0, len(deleted))
	for _, id := range deleted {
		ids = append(ids, fmt.Sprint(id))
	}
	sort.Strings(ids)
	return ids
}

func expectedHundredEventIDs() []string {
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = fmt.Sprintf("event-%03d", i)
	}
	return ids
}

func sqsEntryCountByQueue(requests []fakeSQSRequest) map[string]int {
	counts := map[string]int{}
	for _, request := range requests {
		counts[request.queueURL] += len(request.entries)
	}
	return counts
}

func sqsRequestCountByQueue(requests []fakeSQSRequest) map[string]int {
	counts := map[string]int{}
	for _, request := range requests {
		counts[request.queueURL]++
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

func strconvNano() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
