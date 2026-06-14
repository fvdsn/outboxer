package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/api/option"
)

const (
	runModePoll     = "poll"
	runModeOnce     = "once"
	runModeOnDemand = "ondemand"

	eventTargetSQS      = "sqs"
	sqsEventBatchSize   = 10
	sqsEventMaxSizeByte = 256 * 1024
)

type appConfig struct {
	EventTable       string
	EventID          string
	EventTimestamp   string
	EventData        string
	EventTarget      string
	EventTopic       string
	EventOrderingKey string
	EventAttributes  string

	BatchSize          int
	BatchWorkers       int
	BatchMaxSequential int

	DeadlockCheckInterval time.Duration
	HealthcheckPort       int
	DefaultTopic          string
	PubSubAPIEndpoint     string
	ErrorCooldown         time.Duration
	RunMode               string

	PGHost                  string
	PGPort                  uint16
	PGUser                  string
	PGPassword              string
	PGDatabase              string
	PGSSL                   bool
	PGSSLRejectUnauthorized bool
	PGTimeout               time.Duration
	PGMaxConnections        int

	AWSRegion                  string
	AWSRoleARN                 string
	AWSRoleSessionName         string
	AWSRoleDuration            time.Duration
	AWSCredentialRefreshWindow time.Duration
}

type app struct {
	cfg    appConfig
	db     *sql.DB
	pubsub pubsubPublisher
	sqs    sqsPublisher

	txMu sync.Mutex
}

type pubsubPublisher interface {
	Publish(ctx context.Context, message pubsubMessage) (string, error)
}

type pubsubMessage struct {
	Topic       string
	Data        []byte
	OrderingKey string
	Attributes  map[string]string
}

type sqsPublisher interface {
	SendBatch(ctx context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error)
}

type sqsBatchEntry struct {
	ID             string
	MessageBody    string
	Attributes     map[string]string
	MessageGroupID string
}

type sqsBatchResponse struct {
	Successful []sqsBatchSuccess
	Failed     []sqsBatchFailure
}

type sqsBatchSuccess struct {
	ID        string
	MessageID string
}

type sqsBatchFailure struct {
	ID          string
	Code        string
	Message     string
	SenderFault bool
}

type cloudPubSubPublisher struct {
	client *pubsub.Client
}

type awsSQSPublisher struct {
	client *sqs.Client
}

type event struct {
	columns map[string]any
}

var (
	randomMu                 sync.Mutex
	randomSource             = rand.New(rand.NewSource(time.Now().UnixNano()))
	deadlockDetector         = randomInt63()
	deadlockDetectorPrevious int64
)

func main() {
	loadDotEnv(".env")
	cfg := loadConfig()

	ctx := context.Background()

	startDeadlockDetector(cfg.DeadlockCheckInterval)

	db, err := openDB(cfg)
	if err != nil {
		logError(map[string]any{"message": "Something is wrong with the database", "error": err.Error()})
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}

	pubsubClient, err := newPubSubClient(ctx, cfg)
	if err != nil {
		logError(map[string]any{"message": "Failed to create pubsub client", "error": err.Error()})
		os.Exit(1)
	}
	defer pubsubClient.Close()

	sqsClient, err := newSQSClient(ctx, cfg)
	if err != nil {
		logError(map[string]any{"message": "Failed to create sqs client", "error": err.Error()})
		os.Exit(1)
	}

	a := &app{
		cfg:    cfg,
		db:     db,
		pubsub: &cloudPubSubPublisher{client: pubsubClient},
		sqs:    &awsSQSPublisher{client: sqsClient},
	}

	logInfo(map[string]any{"message": "Startup", "pid": os.Getpid()})

	if err := a.checkDBWorks(ctx); err != nil {
		logError(map[string]any{"message": "Something is wrong with the database", "error": err.Error()})
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}

	go handleSignals(db)

	switch cfg.RunMode {
	case runModePoll:
		server := a.serveHTTPRequests()
		if err := a.processEvents(ctx, cfg.RunMode); err != nil {
			logError(map[string]any{"message": "crashed and exited", "error": err.Error()})
		} else {
			logError(map[string]any{"message": "crashed and exited"})
		}
		_ = db.Close()
		_ = server.Close()
		os.Exit(1)
	case runModeOnDemand:
		a.serveHTTPRequests()
		select {}
	default:
		_ = a.processEvents(ctx, cfg.RunMode)
		_ = db.Close()
		logInfo(map[string]any{"message": "done"})
	}
}

func loadConfig() appConfig {
	return appConfig{
		EventTable:       getenv("EVENT_TABLE", "events"),
		EventID:          getenv("EVENT_ID", "id"),
		EventTimestamp:   getenv("EVENT_TIMESTAMP", "timestamp"),
		EventData:        getenv("EVENT_DATA", "data"),
		EventTarget:      getenv("EVENT_TARGET", "target"),
		EventTopic:       getenv("EVENT_TOPIC", "topic"),
		EventOrderingKey: getenv("EVENT_ORDERING_KEY", "ordering_key"),
		EventAttributes:  getenv("EVENT_ATTRIBUTES", "attributes"),

		BatchSize:          getenvInt("BATCH_SIZE", 32),
		BatchWorkers:       getenvInt("BATCH_WORKERS", 8),
		BatchMaxSequential: getenvInt("BATCH_MAX_SEQUENTIAL", 8),

		DeadlockCheckInterval: time.Duration(getenvInt("DEADLOCK_CHECK_INTERVAL_SEC", 10*60)) * time.Second,
		HealthcheckPort:       getenvInt("HEALTHCHECK_PORT", 9000+int(randomInt63()%1000)),
		DefaultTopic:          getenv("DEFAULT_TOPIC", "default"),
		PubSubAPIEndpoint:     getenv("PUBSUB_API_ENDPOINT", ""),
		ErrorCooldown:         time.Duration(getenvInt("ERROR_COOLDOWN_MS", 5000)) * time.Millisecond,
		RunMode:               getenv("RUN_MODE", runModePoll),

		PGHost:                  getenv("PG_HOST", "0.0.0.0"),
		PGPort:                  uint16(getenvInt("PG_PORT", 5432)),
		PGUser:                  getenv("PG_USER", "fred"),
		PGPassword:              getenv("PG_PASSWORD", ""),
		PGDatabase:              getenv("PG_DATABASE", "fred"),
		PGSSL:                   os.Getenv("PG_SSL") == "true",
		PGSSLRejectUnauthorized: os.Getenv("PG_SSL_REJECT_UNAUTHORIZED") == "true",
		PGTimeout:               time.Duration(getenvInt("PG_TIMEOUT", 10000)) * time.Millisecond,
		PGMaxConnections:        getenvInt("PG_MAX_CONNECTIONS", 10),

		AWSRegion:                  getenv("AWS_REGION", ""),
		AWSRoleARN:                 getenv("AWS_ROLE_ARN", ""),
		AWSRoleSessionName:         getenv("AWS_ROLE_SESSION_NAME", "core-outbox"),
		AWSRoleDuration:            time.Duration(getenvInt("AWS_ROLE_DURATION_SECONDS", 3600)) * time.Second,
		AWSCredentialRefreshWindow: time.Duration(getenvInt("AWS_CREDENTIAL_REFRESH_WINDOW_MS", 5*60*1000)) * time.Millisecond,
	}
}

func loadDotEnv(path string) {
	content, err := os.ReadFile(path)
	if err != nil {
		return
	}

	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		_ = os.Setenv(key, value)
	}
}

func getenv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func openDB(cfg appConfig) (*sql.DB, error) {
	pgConfig, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	pgConfig.Host = cfg.PGHost
	pgConfig.Port = cfg.PGPort
	pgConfig.User = cfg.PGUser
	pgConfig.Password = cfg.PGPassword
	pgConfig.Database = cfg.PGDatabase
	pgConfig.ConnectTimeout = cfg.PGTimeout

	if cfg.PGSSL {
		pgConfig.TLSConfig = &tls.Config{InsecureSkipVerify: !cfg.PGSSLRejectUnauthorized}
	} else {
		pgConfig.TLSConfig = nil
	}

	db := stdlib.OpenDB(*pgConfig)
	db.SetMaxOpenConns(cfg.PGMaxConnections)
	db.SetMaxIdleConns(0)
	return db, nil
}

func newPubSubClient(ctx context.Context, cfg appConfig) (*pubsub.Client, error) {
	options := []option.ClientOption{}
	if cfg.PubSubAPIEndpoint != "" {
		options = append(options, option.WithEndpoint(cfg.PubSubAPIEndpoint))
	}
	return pubsub.NewClient(ctx, "", options...)
}

func newSQSClient(ctx context.Context, cfg appConfig) (*sqs.Client, error) {
	loadOptions := []func(*config.LoadOptions) error{}
	if cfg.AWSRegion != "" {
		loadOptions = append(loadOptions, config.WithRegion(cfg.AWSRegion))
	}

	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, err
	}

	if cfg.AWSRoleARN != "" {
		stsClient := sts.NewFromConfig(awsConfig)
		provider := stscreds.NewAssumeRoleProvider(stsClient, cfg.AWSRoleARN, func(options *stscreds.AssumeRoleOptions) {
			options.RoleSessionName = cfg.AWSRoleSessionName
			options.Duration = cfg.AWSRoleDuration
		})
		awsConfig.Credentials = aws.NewCredentialsCache(provider, func(options *aws.CredentialsCacheOptions) {
			options.ExpiryWindow = cfg.AWSCredentialRefreshWindow
		})
	}

	return sqs.NewFromConfig(awsConfig), nil
}

func (p *cloudPubSubPublisher) Publish(ctx context.Context, message pubsubMessage) (string, error) {
	topic := p.client.Topic(message.Topic)
	if message.OrderingKey != "" {
		topic.EnableMessageOrdering = true
	}

	pubsubMsg := &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	}
	if message.OrderingKey != "" {
		pubsubMsg.OrderingKey = message.OrderingKey
	}

	return topic.Publish(ctx, pubsubMsg).Get(ctx)
}

func (p *awsSQSPublisher) SendBatch(ctx context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	awsEntries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(entries))
	for _, entry := range entries {
		awsEntry := sqstypes.SendMessageBatchRequestEntry{
			Id:                aws.String(entry.ID),
			MessageBody:       aws.String(entry.MessageBody),
			MessageAttributes: convertAttributesToAWSSQS(entry.Attributes),
		}
		if entry.MessageGroupID != "" {
			awsEntry.MessageGroupId = aws.String(entry.MessageGroupID)
		}
		awsEntries = append(awsEntries, awsEntry)
	}

	response, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  awsEntries,
	})
	if err != nil {
		return sqsBatchResponse{}, err
	}

	converted := sqsBatchResponse{}
	for _, entry := range response.Successful {
		converted.Successful = append(converted.Successful, sqsBatchSuccess{
			ID:        aws.ToString(entry.Id),
			MessageID: aws.ToString(entry.MessageId),
		})
	}
	for _, entry := range response.Failed {
		converted.Failed = append(converted.Failed, sqsBatchFailure{
			ID:          aws.ToString(entry.Id),
			Code:        aws.ToString(entry.Code),
			Message:     aws.ToString(entry.Message),
			SenderFault: entry.SenderFault,
		})
	}
	return converted, nil
}

func handleSignals(db *sql.DB) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	logInfo(map[string]any{"message": "Shutdown requested by host"})
	_ = db.Close()
	logInfo(map[string]any{"message": "Graceful shutdown"})
	os.Exit(0)
}

func startDeadlockDetector(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if deadlockDetector == deadlockDetectorPrevious {
				logError(map[string]any{"message": "deadlock detected, shutting down"})
				os.Exit(1)
			}
			deadlockDetectorPrevious = deadlockDetector
			logInfo(map[string]any{"message": "all good"})
		}
	}()
}

func (a *app) checkDBWorks(ctx context.Context) error {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", ident(a.cfg.EventTable))
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (a *app) processEvents(ctx context.Context, mode string) error {
	logInfo(map[string]any{"message": fmt.Sprintf("Processing events from table '%s'", a.cfg.EventTable)})

	for {
		tx, err := a.db.BeginTx(ctx, nil)
		if err != nil {
			logError(map[string]any{"message": "Error while starting event batch transaction", "error": err.Error()})
			if a.cfg.RunMode == runModeOnDemand {
				return err
			}
			time.Sleep(a.cfg.ErrorCooldown)
		} else {
			if err := a.processEventBatch(ctx, tx); err != nil {
				logError(map[string]any{"message": "Error during event batch transaction", "error": err.Error()})
				time.Sleep(a.cfg.ErrorCooldown)
			}
			if err := tx.Commit(); err != nil {
				logError(map[string]any{"message": "Error while starting event batch transaction", "error": err.Error()})
				if a.cfg.RunMode == runModeOnDemand {
					return err
				}
				time.Sleep(a.cfg.ErrorCooldown)
			}
		}

		if mode != runModePoll {
			break
		}
	}

	return nil
}

func (a *app) processEventBatch(ctx context.Context, tx *sql.Tx) error {
	deadlockDetector = randomInt63()

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		return err
	}
	if len(events) > 0 {
		logInfo(map[string]any{"message": fmt.Sprintf("processing %d messages", len(events))})
	}

	var idsMu sync.Mutex
	idsToDelete := []any{}
	addIDToDelete := func(id any) {
		idsMu.Lock()
		defer idsMu.Unlock()
		idsToDelete = append(idsToDelete, id)
	}

	jobs := parallelizeEvents(a.cfg, events)
	errs := make(chan error, len(jobs))
	var wg sync.WaitGroup

	for _, jobEvents := range jobs {
		jobEvents := jobEvents
		wg.Add(1)
		go func() {
			defer wg.Done()

			pubsubEvents := []event{}
			sqsEvents := []event{}
			for _, evt := range jobEvents {
				if eventString(evt, a.cfg.EventTarget) == eventTargetSQS {
					sqsEvents = append(sqsEvents, evt)
				} else {
					pubsubEvents = append(pubsubEvents, evt)
				}
			}

			if err := a.sendPubsubEvents(ctx, tx, pubsubEvents, addIDToDelete); err != nil {
				errs <- err
				return
			}
			if err := a.sendSQSEvents(ctx, tx, sqsEvents, addIDToDelete); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	idsMu.Lock()
	deleteIDs := append([]any(nil), idsToDelete...)
	idsMu.Unlock()

	if err := a.deleteEvents(ctx, tx, deleteIDs); err != nil {
		return err
	}

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *app) selectEvents(ctx context.Context, tx *sql.Tx) ([]event, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY %s LIMIT $1 FOR UPDATE",
		ident(a.cfg.EventTable),
		ident(a.cfg.EventID),
	)

	rows, err := tx.QueryContext(ctx, query, a.cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	events := []event{}
	for rows.Next() {
		values := make([]any, len(columns))
		valuePointers := make([]any, len(columns))
		for i := range values {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			return nil, err
		}

		evt := event{columns: map[string]any{}}
		for i, column := range columns {
			evt.columns[column] = normalizeDBValue(values[i])
		}
		events = append(events, evt)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func normalizeDBValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		copied := make([]byte, len(typed))
		copy(copied, typed)
		return copied
	default:
		return typed
	}
}

func (a *app) sendPubsubEvents(ctx context.Context, tx *sql.Tx, events []event, addIDToDelete func(any)) error {
	for _, evt := range events {
		if err := a.sendPubsubEvent(ctx, tx, evt, addIDToDelete); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) sendPubsubEvent(ctx context.Context, tx *sql.Tx, evt event, addIDToDelete func(any)) error {
	target := eventOptionalString(evt, a.cfg.EventTarget)
	topicName := eventString(evt, a.cfg.EventTopic)
	if topicName == "" {
		topicName = a.cfg.DefaultTopic
	}
	orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
	attributes := eventAttributes(evt, a.cfg.EventAttributes)
	timestamp := eventValue(evt, a.cfg.EventTimestamp)
	id := eventValue(evt, a.cfg.EventID)
	data := eventBytes(evt, a.cfg.EventData)
	latency := eventLatency(timestamp)

	logDebug(map[string]any{
		"message":          "Sending event",
		"eventId":          id,
		"eventTimestamp":   timestamp,
		"eventLatency":     latency,
		"eventPayloadSize": len(data),
		"eventOrderingKey": orderingKey,
		"eventAttributes":  attributes,
		"eventTarget":      target,
		"eventTopic":       topicName,
	})

	start := time.Now()
	stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
	if len(deletedAttributes) != 0 {
		logError(map[string]any{
			"message":           "Some attributes were deleted",
			"eventId":           id,
			"eventTopic":        topicName,
			"deletedAttributes": deletedAttributes,
		})
	}

	messageID, err := a.pubsub.Publish(ctx, pubsubMessage{
		Topic:       topicName,
		Data:        data,
		OrderingKey: orderingKey,
		Attributes:  stringAttributes,
	})
	if err != nil {
		logError(map[string]any{
			"message":          "Failed to send event",
			"eventId":          id,
			"eventOrderingKey": orderingKey,
			"eventAttributes":  stringAttributes,
			"eventTarget":      target,
			"eventTopic":       topicName,
			"error":            err.Error(),
		})
		return err
	}

	pubsubLatency := time.Since(start).Seconds()
	if orderingKey != "" {
		a.txMu.Lock()
		err = a.deleteEvent(ctx, tx, id)
		a.txMu.Unlock()
		if err != nil {
			return err
		}
	} else {
		addIDToDelete(id)
	}

	logDebug(map[string]any{
		"message":          "Event sent",
		"eventId":          id,
		"eventTimestamp":   timestamp,
		"eventLatency":     latency,
		"eventPayloadSize": len(data),
		"eventPublishedId": messageID,
		"eventOrderingKey": orderingKey,
		"eventAttributes":  stringAttributes,
		"eventTarget":      target,
		"eventTopic":       topicName,
		"pubsubLatency":    pubsubLatency,
	})

	return nil
}

func (a *app) sendSQSEvents(ctx context.Context, tx *sql.Tx, events []event, addIDToDelete func(any)) error {
	eventsByQueue := map[string][]event{}
	for _, evt := range events {
		queue := eventString(evt, a.cfg.EventTopic)
		eventsByQueue[queue] = append(eventsByQueue[queue], evt)
	}

	for queue, queueEvents := range eventsByQueue {
		for i := 0; i < len(queueEvents); i += sqsEventBatchSize {
			end := i + sqsEventBatchSize
			if end > len(queueEvents) {
				end = len(queueEvents)
			}
			if err := a.sendSQS10Events(ctx, tx, queue, queueEvents[i:end], addIDToDelete); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *app) sendSQS10Events(ctx context.Context, tx *sql.Tx, queueURL string, events []event, addIDToDelete func(any)) error {
	if len(events) == 0 {
		return nil
	}

	isFIFO := false
	for _, evt := range events {
		if eventOptionalString(evt, a.cfg.EventOrderingKey) != "" {
			isFIFO = true
			break
		}
	}

	start := time.Now()
	entries := []sqsBatchEntry{}
	idsByEntryID := map[string]any{}

	for _, evt := range events {
		orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
		attributes := eventAttributes(evt, a.cfg.EventAttributes)
		timestamp := eventValue(evt, a.cfg.EventTimestamp)
		id := eventValue(evt, a.cfg.EventID)
		entryID := fmt.Sprint(id)
		data := eventBytes(evt, a.cfg.EventData)
		latency := eventLatency(timestamp)

		if len(data) >= sqsEventMaxSizeByte {
			a.txMu.Lock()
			err := a.deleteEvent(ctx, tx, id)
			a.txMu.Unlock()
			if err != nil {
				return err
			}

			logError(map[string]any{
				"message":    "Failed to send event",
				"eventId":    id,
				"eventTopic": queueURL,
				"error":      fmt.Sprintf("Event too big: %d bytes", len(data)),
			})
			continue
		}

		logDebug(map[string]any{
			"message":          "Sending event",
			"eventId":          id,
			"eventTimestamp":   timestamp,
			"eventLatency":     latency,
			"eventPayloadSize": len(data),
			"eventOrderingKey": orderingKey,
			"eventAttributes":  attributes,
			"eventTarget":      eventTargetSQS,
			"eventTopic":       queueURL,
		})

		stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
		if len(deletedAttributes) != 0 {
			logError(map[string]any{
				"message":           "Some attributes were deleted",
				"eventId":           id,
				"eventTopic":        queueURL,
				"deletedAttributes": deletedAttributes,
			})
		}

		entry := sqsBatchEntry{
			ID:          entryID,
			MessageBody: string(data),
			Attributes:  stringAttributes,
		}
		if isFIFO {
			groupID := orderingKey
			if groupID == "" {
				groupID = strconv.FormatInt(randomInt63(), 10)
			}
			entry.MessageGroupID = groupID
		}

		entries = append(entries, entry)
		idsByEntryID[entryID] = id
	}

	if len(entries) == 0 {
		return nil
	}

	response, err := a.sqs.SendBatch(ctx, queueURL, entries)
	if err != nil {
		logError(map[string]any{
			"message":    "Failed to send event batch",
			"eventTopic": queueURL,
			"error":      err.Error(),
		})
		return err
	}

	pubsubLatency := time.Since(start).Seconds()
	for _, entry := range response.Successful {
		originalID := idsByEntryID[entry.ID]
		addIDToDelete(originalID)
		logDebug(map[string]any{
			"message":          "Event sent",
			"eventId":          entry.ID,
			"eventPublishedId": entry.MessageID,
			"eventTopic":       queueURL,
			"pubsubLatency":    pubsubLatency,
		})
	}

	for _, entry := range response.Failed {
		if entry.SenderFault {
			addIDToDelete(idsByEntryID[entry.ID])
		}
		logError(map[string]any{
			"message":    "Failed to send event",
			"eventId":    entry.ID,
			"eventTopic": queueURL,
			"error":      fmt.Sprintf("%s: %s", entry.Code, entry.Message),
		})
	}

	return nil
}

func convertAttributesToAWSSQS(attributes map[string]string) map[string]sqstypes.MessageAttributeValue {
	if attributes == nil {
		return nil
	}

	converted := map[string]sqstypes.MessageAttributeValue{}
	for key, value := range attributes {
		converted[key] = sqstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(value),
		}
	}
	return converted
}

func sanitizeStringAttributes(attributes map[string]any) (map[string]string, map[string]any) {
	if attributes == nil {
		return nil, nil
	}

	kept := map[string]string{}
	deleted := map[string]any{}
	for key, value := range attributes {
		stringValue, ok := value.(string)
		if ok {
			kept[key] = stringValue
		} else {
			deleted[key] = value
		}
	}
	return kept, deleted
}

func parallelizeEvents(cfg appConfig, events []event) [][]event {
	jobs := make([][]event, cfg.BatchWorkers)
	seed := int(randomInt63() % 100000)

	for _, evt := range events {
		orderingKey := eventOptionalString(evt, cfg.EventOrderingKey)
		if orderingKey != "" {
			jobIdx := strHash(seed, orderingKey) % cfg.BatchWorkers
			if len(jobs[jobIdx]) >= cfg.BatchMaxSequential {
				continue
			}
			jobs[jobIdx] = append(jobs[jobIdx], evt)
			continue
		}

		jobIdx := 0
		for i := range jobs {
			if len(jobs[i]) < len(jobs[jobIdx]) {
				jobIdx = i
			}
		}
		jobs[jobIdx] = append(jobs[jobIdx], evt)
	}

	return jobs
}

func strHash(seed int, str string) int {
	hash := int32(seed)
	for _, char := range str {
		hash = (hash << 5) - hash + char
	}
	if hash == math.MinInt32 {
		return math.MaxInt32
	}
	if hash < 0 {
		return int(-hash)
	}
	return int(hash)
}

func (a *app) deleteEvent(ctx context.Context, tx *sql.Tx, id any) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", ident(a.cfg.EventTable), ident(a.cfg.EventID))
	_, err := tx.ExecContext(ctx, query, id)
	return err
}

func (a *app) deleteEvents(ctx context.Context, tx *sql.Tx, ids []any) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	for i := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		ident(a.cfg.EventTable),
		ident(a.cfg.EventID),
		strings.Join(placeholders, ", "),
	)

	a.txMu.Lock()
	defer a.txMu.Unlock()
	_, err := tx.ExecContext(ctx, query, ids...)
	return err
}

func (a *app) serveHTTPRequests() *http.Server {
	server := &http.Server{Addr: fmt.Sprintf(":%d", a.cfg.HealthcheckPort)}

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		method := strings.ToUpper(req.Method)
		switch {
		case a.cfg.RunMode == runModeOnDemand && method == http.MethodPost:
			if err := a.processEvents(req.Context(), a.cfg.RunMode); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(err.Error()))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("done"))
		case method == http.MethodDelete:
			origin := req.Header.Get("x-forwarded-for")
			if origin == "" && req.RemoteAddr != "" {
				origin = req.RemoteAddr
			}
			if origin == "" {
				origin = "unknown"
			}
			logInfo(map[string]any{"message": "Shutdown requested by client", "from": origin})
			_ = a.db.Close()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Shutting down"))
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(ctx)
				logInfo(map[string]any{"message": "Graceful shutdown"})
				os.Exit(0)
			}()
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("all good"))
			logInfo(map[string]any{"message": "Healtcheck request answered"})
		}
	})

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logError(map[string]any{"message": "HTTP server failed", "error": err.Error()})
			os.Exit(1)
		}
	}()

	logInfo(map[string]any{"message": fmt.Sprintf("Server listening on http://0.0.0.0:%d", a.cfg.HealthcheckPort)})
	return server
}

func eventValue(evt event, column string) any {
	return evt.columns[column]
}

func eventOptionalString(evt event, column string) string {
	value := eventString(evt, column)
	if value == "" {
		return ""
	}
	return value
}

func eventString(evt event, column string) string {
	value := eventValue(evt, column)
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(typed)
	}
}

func eventBytes(evt event, column string) []byte {
	value := eventValue(evt, column)
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		return []byte(fmt.Sprint(typed))
	}
}

func eventAttributes(evt event, column string) map[string]any {
	value := eventValue(evt, column)
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return typed
	case []byte:
		return parseAttributesJSON(typed)
	case string:
		return parseAttributesJSON([]byte(typed))
	default:
		return nil
	}
}

func parseAttributesJSON(content []byte) map[string]any {
	if len(content) == 0 {
		return nil
	}

	attributes := map[string]any{}
	if err := json.Unmarshal(content, &attributes); err != nil {
		return nil
	}
	return attributes
}

func eventLatency(value any) any {
	var timestamp time.Time
	switch typed := value.(type) {
	case nil:
		return nil
	case time.Time:
		timestamp = typed
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return nil
		}
		timestamp = parsed
	case []byte:
		parsed, err := time.Parse(time.RFC3339Nano, string(typed))
		if err != nil {
			return nil
		}
		timestamp = parsed
	default:
		return nil
	}

	return time.Since(timestamp).Seconds()
}

func ident(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

func randomInt63() int64 {
	randomMu.Lock()
	defer randomMu.Unlock()
	return randomSource.Int63()
}

func logDebug(fields map[string]any) {
	logWithLevel("DEBUG", fields)
}

func logInfo(fields map[string]any) {
	logWithLevel("INFO", fields)
}

func logError(fields map[string]any) {
	logWithLevel("ERROR", fields)
}

func logWithLevel(level string, fields map[string]any) {
	payload := map[string]any{
		"log_level": level,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	for key, value := range fields {
		payload[key] = value
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(`{"log_level":"ERROR","message":"failed to encode log"}`)
		return
	}
	fmt.Println(string(encoded))
}
