package outboxer

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/compute/metadata"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	eventTargetPubSub   = "pubsub"
	eventTargetSQS      = "sqs"
	sqsEventBatchSize   = 10
	sqsEventMaxSizeByte = 1024 * 1024
	sqsMaxAttributes    = 10
	sqsMaxDelaySeconds  = 900

	awsWebIdentityProviderGoogle = "google"
)

var (
	sqsBatchEntryIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	sqsFIFOIDPattern       = regexp.MustCompile(`^[A-Za-z0-9!"#$%&'()*+,\-./:;<=>?@\[\\\]\^_` + "`" + `{|}~]{1,128}$`)
	sqsAttributeNameRe     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
)

type sqsPublisher interface {
	SendBatch(ctx context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error)
}

type sqsBatchEntry struct {
	ID                 string
	MessageBody        string
	Attributes         map[string]sqsMessageAttribute
	MessageGroupID     string
	DeduplicationID    string
	DelaySeconds       *int32
	AWSXRayTraceHeader string
}

type sqsCandidateEvent struct {
	evt         event
	options     backendOptions
	id          any
	orderingKey string
}

func (evt sqsCandidateEvent) fifoGroupID() string {
	if evt.orderingKey != "" {
		return evt.orderingKey
	}
	return syntheticFIFOGroupID(fmt.Sprint(evt.id))
}

type sqsPreparedEvent struct {
	id          any
	timestamp   any
	latency     any
	payloadLen  int
	messageSize int
	orderingKey string
	entry       sqsBatchEntry
}

type sqsQueueEvents struct {
	queue  string
	events []event
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

type awsSQSPublisher struct {
	client *sqs.Client
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

	switch {
	case cfg.AWSWebIdentityProvider == awsWebIdentityProviderGoogle:
		// GCP to AWS: assume the role with a Google OIDC token fetched from the
		// GCP metadata server, instead of using the AWS default credential chain.
		stsClient := sts.NewFromConfig(awsConfig)
		retriever := &googleIDTokenRetriever{ctx: ctx, audience: cfg.AWSWebIdentityAudience}
		provider := stscreds.NewWebIdentityRoleProvider(stsClient, cfg.AWSRoleARN, retriever, func(options *stscreds.WebIdentityRoleOptions) {
			options.RoleSessionName = cfg.AWSRoleSessionName
			options.Duration = cfg.AWSRoleDuration
		})
		awsConfig.Credentials = aws.NewCredentialsCache(provider, func(options *aws.CredentialsCacheOptions) {
			options.ExpiryWindow = cfg.AWSCredentialRefreshWindow
		})
	case cfg.AWSRoleARN != "":
		stsClient := sts.NewFromConfig(awsConfig)
		provider := stscreds.NewAssumeRoleProvider(stsClient, cfg.AWSRoleARN, func(options *stscreds.AssumeRoleOptions) {
			options.RoleSessionName = cfg.AWSRoleSessionName
			options.Duration = cfg.AWSRoleDuration
		})
		awsConfig.Credentials = aws.NewCredentialsCache(provider, func(options *aws.CredentialsCacheOptions) {
			options.ExpiryWindow = cfg.AWSCredentialRefreshWindow
		})
	}

	clientOptions := []func(*sqs.Options){}
	if cfg.SQSAPIEndpoint != "" {
		clientOptions = append(clientOptions, func(options *sqs.Options) {
			options.BaseEndpoint = aws.String(cfg.SQSAPIEndpoint)
		})
	}

	return sqs.NewFromConfig(awsConfig, clientOptions...), nil
}

// googleIDTokenRetriever fetches a Google-signed OIDC ID token from the GCP
// metadata server for use as an AWS web identity token. It works on Cloud Run,
// GCE, and GKE with Workload Identity.
type googleIDTokenRetriever struct {
	ctx      context.Context
	audience string
}

func (r *googleIDTokenRetriever) GetIdentityToken() ([]byte, error) {
	path := fmt.Sprintf("instance/service-accounts/default/identity?audience=%s&format=full", url.QueryEscape(r.audience))
	token, err := metadata.GetWithContext(r.ctx, path)
	if err != nil {
		return nil, fmt.Errorf("fetch Google ID token from metadata server: %w", err)
	}
	return []byte(token), nil
}

func (p *awsSQSPublisher) SendBatch(ctx context.Context, queueURL string, entries []sqsBatchEntry) (sqsBatchResponse, error) {
	awsEntries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(entries))
	for _, entry := range entries {
		awsEntry := sqstypes.SendMessageBatchRequestEntry{
			Id:                aws.String(entry.ID),
			MessageBody:       aws.String(entry.MessageBody),
			MessageAttributes: convertAttributesToAWSSQS(entry.Attributes),
		}
		if entry.DelaySeconds != nil {
			awsEntry.DelaySeconds = *entry.DelaySeconds
		}
		if entry.MessageGroupID != "" {
			awsEntry.MessageGroupId = aws.String(entry.MessageGroupID)
		}
		if entry.DeduplicationID != "" {
			awsEntry.MessageDeduplicationId = aws.String(entry.DeduplicationID)
		}
		if entry.AWSXRayTraceHeader != "" {
			awsEntry.MessageSystemAttributes = map[string]sqstypes.MessageSystemAttributeValue{
				"AWSTraceHeader": {
					DataType:    aws.String("String"),
					StringValue: aws.String(entry.AWSXRayTraceHeader),
				},
			}
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

func (a *app) sendSQSEventsWithCallbacks(ctx context.Context, events []event, callbacks senderCallbacks) error {
	eventsByQueue := map[string][]event{}
	for _, evt := range events {
		queue := evt.route.destination
		eventsByQueue[queue] = append(eventsByQueue[queue], evt)
	}

	queueGroups := make([]sqsQueueEvents, 0, len(eventsByQueue))
	for queue, queueEvents := range eventsByQueue {
		queueGroups = append(queueGroups, sqsQueueEvents{queue: queue, events: append([]event(nil), queueEvents...)})
	}

	sem := make(chan struct{}, a.cfg.SQSSendConcurrency)
	return runConcurrent(queueGroups, func(group sqsQueueEvents) error {
		return a.sendSQSQueueEvents(ctx, sem, group.queue, group.events, callbacks)
	})
}

func (a *app) sendSQSQueueEvents(ctx context.Context, sem chan struct{}, queue string, events []event, callbacks senderCallbacks) error {
	if !validSQSQueueURL(queue) {
		for _, evt := range events {
			callbacks.addPoisonID(eventValue(evt, a.cfg.EventID), "SQS queue URL is syntactically invalid")
		}
		a.logFailure(ctx, "Failed to send event batch",
			fmt.Sprintf("sqs|%s|invalid-queue-url", queue),
			"event_destination", queue,
			"error", "SQS queue URL is syntactically invalid",
		)
		return nil
	}

	candidates := make([]sqsCandidateEvent, 0, len(events))
	for _, evt := range events {
		candidate, ok := a.parseSQSCandidate(ctx, evt, queue, callbacks)
		if ok {
			candidates = append(candidates, candidate)
		}
	}

	if strings.HasSuffix(queue, ".fifo") {
		return a.sendSQSFIFOEvents(ctx, sem, queue, candidates, callbacks)
	}

	prepared := make([]sqsPreparedEvent, 0, len(candidates))
	for _, candidate := range candidates {
		evt, ok := a.prepareSQSEvent(ctx, candidate, queue, false, callbacks)
		if ok {
			prepared = append(prepared, evt)
		}
	}
	return a.sendSQSStandardEvents(ctx, sem, queue, prepared, callbacks)
}

func (a *app) sendSQSStandardEvents(ctx context.Context, sem chan struct{}, queue string, queueEvents []sqsPreparedEvent, callbacks senderCallbacks) error {
	chunks := chunkSQSStandardEvents(queueEvents)
	return runConcurrent(chunks, func(chunk []sqsPreparedEvent) error {
		batch := append([]sqsPreparedEvent(nil), chunk...)
		_, err := a.sendSQSBatchWithSemaphore(ctx, sem, queue, batch, callbacks)
		return err
	})
}

func chunkSQSStandardEvents(events []sqsPreparedEvent) [][]sqsPreparedEvent {
	chunks := [][]sqsPreparedEvent{}
	current := []sqsPreparedEvent{}
	currentSize := 0

	for _, evt := range events {
		size := evt.messageSize
		if len(current) > 0 && (len(current) >= sqsEventBatchSize || currentSize+size > sqsEventMaxSizeByte) {
			chunks = append(chunks, current)
			current = nil
			currentSize = 0
		}
		current = append(current, evt)
		currentSize += size
	}

	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

func (a *app) sendSQSFIFOEvents(ctx context.Context, sem chan struct{}, queue string, queueEvents []sqsCandidateEvent, callbacks senderCallbacks) error {
	groups := map[string][]sqsCandidateEvent{}
	groupOrder := []string{}
	for _, evt := range queueEvents {
		groupID := evt.fifoGroupID()
		if _, ok := groups[groupID]; !ok {
			groupOrder = append(groupOrder, groupID)
		}
		groups[groupID] = append(groups[groupID], evt)
	}

	return runConcurrent(groupOrder, func(groupID string) error {
		groupEvents := append([]sqsCandidateEvent(nil), groups[groupID]...)
		for _, candidate := range groupEvents {
			prepared, ok := a.prepareSQSEvent(ctx, candidate, queue, true, callbacks)
			if !ok {
				continue
			}
			done, err := a.sendSQSBatchWithSemaphore(ctx, sem, queue, []sqsPreparedEvent{prepared}, callbacks)
			if err != nil {
				return err
			}
			if !done {
				return nil
			}
		}
		return nil
	})
}

func (a *app) parseSQSCandidate(ctx context.Context, evt event, queueURL string, callbacks senderCallbacks) (sqsCandidateEvent, bool) {
	options, err := eventSQSOptions(evt, a.cfg)
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, eventTargetSQS, queueURL, "", err, callbacks)
		return sqsCandidateEvent{}, false
	}
	orderingKey, err := options.stringValue("messageGroupId")
	if err != nil {
		a.rejectMalformedOptions(ctx, evt, eventTargetSQS, queueURL, "messageGroupId", err, callbacks)
		return sqsCandidateEvent{}, false
	}

	id := eventValue(evt, a.cfg.EventID)
	return sqsCandidateEvent{
		evt:         evt,
		options:     options,
		id:          id,
		orderingKey: orderingKey,
	}, true
}

func (a *app) prepareSQSEvent(ctx context.Context, candidate sqsCandidateEvent, queueURL string, isFIFO bool, callbacks senderCallbacks) (sqsPreparedEvent, bool) {
	attributes, err := sqsAttributes(candidate.options)
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, eventTargetSQS, queueURL, "attributes", err, callbacks)
		return sqsPreparedEvent{}, false
	}
	deduplicationID, err := candidate.options.stringValue("messageDeduplicationId")
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, eventTargetSQS, queueURL, "messageDeduplicationId", err, callbacks)
		return sqsPreparedEvent{}, false
	}
	delaySeconds, err := sqsDelaySeconds(candidate.options)
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, eventTargetSQS, queueURL, "delaySeconds", err, callbacks)
		return sqsPreparedEvent{}, false
	}
	traceHeader, err := sqsAWSTraceHeader(candidate.options)
	if err != nil {
		a.rejectMalformedOptions(ctx, candidate.evt, eventTargetSQS, queueURL, "messageSystemAttributes", err, callbacks)
		return sqsPreparedEvent{}, false
	}

	timestamp := eventValue(candidate.evt, a.cfg.EventTimestamp)
	eventID := fmt.Sprint(candidate.id)
	entryID := providerSafeID(eventID, sqsBatchEntryIDPattern)
	data := eventBytes(candidate.evt, a.cfg.EventPayload)
	latency := eventLatency(timestamp)
	if isSQSPoison(data, attributes, candidate.orderingKey, deduplicationID, delaySeconds) {
		callbacks.addPoisonID(candidate.id, "Event is invalid for SQS")
		a.logFailure(ctx, "Failed to send event",
			fmt.Sprintf("sqs|%s|%s|local-poison", queueURL, candidate.orderingKey),
			"event_id", candidate.id,
			"event_destination", queueURL,
			"error", "Event is invalid for SQS",
		)
		return sqsPreparedEvent{}, false
	}

	entry := sqsBatchEntry{
		ID:                 entryID,
		MessageBody:        string(data),
		Attributes:         attributes,
		AWSXRayTraceHeader: traceHeader,
	}
	if isFIFO {
		entry.MessageGroupID = candidate.fifoGroupID()
		if deduplicationID != "" {
			entry.DeduplicationID = deduplicationID
		} else {
			entry.DeduplicationID = providerSafeID(eventID, sqsFIFOIDPattern)
		}
	} else {
		entry.DelaySeconds = delaySeconds
		entry.MessageGroupID = candidate.orderingKey
	}

	return sqsPreparedEvent{
		id:          candidate.id,
		timestamp:   timestamp,
		latency:     latency,
		payloadLen:  len(data),
		messageSize: sqsMessageSize(data, attributes),
		orderingKey: candidate.orderingKey,
		entry:       entry,
	}, true
}

func (a *app) sendSQSBatchWithSemaphore(ctx context.Context, sem chan struct{}, queue string, events []sqsPreparedEvent, callbacks senderCallbacks) (bool, error) {
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return a.sendSQSBatch(ctx, queue, events, callbacks)
}

func (a *app) sendSQSBatch(ctx context.Context, queueURL string, events []sqsPreparedEvent, callbacks senderCallbacks) (bool, error) {
	if len(events) == 0 {
		return false, nil
	}
	defer markProcessorProgress()

	start := time.Now()
	entries := make([]sqsBatchEntry, 0, len(events))
	idsByEntryID := map[string]any{}

	for _, evt := range events {
		slog.Debug("Sending event",
			"event_id", evt.id,
			"event_timestamp", evt.timestamp,
			"event_latency", evt.latency,
			"event_payload_size", evt.payloadLen,
			"event_ordering_key", evt.orderingKey,
			"event_attributes", evt.entry.Attributes,
			"event_target", eventTargetSQS,
			"event_destination", queueURL,
		)
		entries = append(entries, evt.entry)
		idsByEntryID[evt.entry.ID] = evt.id
	}

	sendCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout)
	defer cancel()
	response, err := a.sqs.SendBatch(sendCtx, queueURL, entries)
	if err != nil {
		if isSQSPermanentRequestError(err) {
			if len(events) == 1 {
				callbacks.addPoisonID(events[0].id, err.Error())
				a.logFailure(ctx, "Failed to send event",
					fmt.Sprintf("sqs|%s|%s", queueURL, err.Error()),
					"event_id", events[0].id,
					"event_destination", queueURL,
					"error", err.Error(),
				)
				return true, nil
			}
			return a.sendSQSBatchIsolated(ctx, queueURL, events, callbacks)
		}
		a.logFailure(ctx, "Failed to send event batch",
			fmt.Sprintf("sqs|%s|%s", queueURL, err.Error()),
			"event_destination", queueURL,
			"error", err.Error(),
		)
		return false, err
	}

	publishLatency := time.Since(start).Seconds()
	anyDone := false
	for _, entry := range response.Successful {
		originalID := idsByEntryID[entry.ID]
		callbacks.addConfirmedID(originalID)
		anyDone = true
		slog.Debug("Event sent",
			"event_id", entry.ID,
			"event_published_id", entry.MessageID,
			"event_destination", queueURL,
			"publish_latency", publishLatency,
		)
	}

	for _, entry := range response.Failed {
		if entry.SenderFault {
			callbacks.addPoisonID(idsByEntryID[entry.ID], fmt.Sprintf("%s: %s", entry.Code, entry.Message))
			anyDone = true
		}
		a.logFailure(ctx, "Failed to send event",
			fmt.Sprintf("sqs|%s|%s|%s", queueURL, entry.Code, entry.Message),
			"event_id", entry.ID,
			"event_destination", queueURL,
			"error", fmt.Sprintf("%s: %s", entry.Code, entry.Message),
		)
	}

	return anyDone, nil
}

func (a *app) sendSQSBatchIsolated(ctx context.Context, queueURL string, events []sqsPreparedEvent, callbacks senderCallbacks) (bool, error) {
	anyDone := false
	var joined error
	for _, evt := range events {
		done, err := a.sendSQSBatch(ctx, queueURL, []sqsPreparedEvent{evt}, callbacks)
		if done {
			anyDone = true
		}
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return anyDone, joined
}

type sqsMessageAttribute struct {
	DataType         string
	StringValue      string
	BinaryValue      []byte
	StringListValues []string
	BinaryListValues [][]byte
	HasStringValue   bool
	HasBinaryValue   bool
	HasStringList    bool
	HasBinaryList    bool
}

func convertAttributesToAWSSQS(attributes map[string]sqsMessageAttribute) map[string]sqstypes.MessageAttributeValue {
	if attributes == nil {
		return nil
	}

	converted := map[string]sqstypes.MessageAttributeValue{}
	for key, value := range attributes {
		attribute := sqstypes.MessageAttributeValue{DataType: aws.String(value.DataType)}
		if value.HasStringValue {
			attribute.StringValue = aws.String(value.StringValue)
		}
		if value.HasBinaryValue {
			attribute.BinaryValue = value.BinaryValue
		}
		if value.HasStringList {
			attribute.StringListValues = value.StringListValues
		}
		if value.HasBinaryList {
			attribute.BinaryListValues = value.BinaryListValues
		}
		converted[key] = attribute
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

func isSQSPoison(body []byte, attributes map[string]sqsMessageAttribute, orderingKey string, deduplicationID string, delaySeconds *int32) bool {
	if len(body) == 0 || !sqsAllowedUnicodeBytes(body) {
		return true
	}
	if sqsMessageSize(body, attributes) > sqsEventMaxSizeByte {
		return true
	}
	if !validSQSAttributes(attributes) {
		return true
	}
	if orderingKey != "" && !sqsFIFOIDPattern.MatchString(orderingKey) {
		return true
	}
	if deduplicationID != "" && !sqsFIFOIDPattern.MatchString(deduplicationID) {
		return true
	}
	if delaySeconds != nil && (*delaySeconds < 0 || *delaySeconds > sqsMaxDelaySeconds) {
		return true
	}
	return false
}

func sqsMessageSize(body []byte, attributes map[string]sqsMessageAttribute) int {
	size := len(body)
	for key, value := range attributes {
		size += len(key) + len(value.DataType)
		if value.HasStringValue {
			size += len(value.StringValue)
		}
		if value.HasBinaryValue {
			size += len(value.BinaryValue)
		}
		for _, item := range value.StringListValues {
			size += len(item)
		}
		for _, item := range value.BinaryListValues {
			size += len(item)
		}
	}
	return size
}

func sqsAttributes(options backendOptions) (map[string]sqsMessageAttribute, error) {
	value, ok := options.values["attributes"]
	if !ok || value == nil {
		return nil, nil
	}
	attributes, ok := normalizeObject(value)
	if !ok {
		return nil, fmt.Errorf("%w: attributes must be an object", errMalformedOptions)
	}
	converted := map[string]sqsMessageAttribute{}
	for name, raw := range attributes {
		attr, err := sqsMessageAttributeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: attribute %s: %w", errMalformedOptions, name, err)
		}
		converted[name] = attr
	}
	return converted, nil
}

func sqsMessageAttributeValue(value any) (sqsMessageAttribute, error) {
	object, ok := normalizeObject(value)
	if !ok {
		return sqsMessageAttribute{}, fmt.Errorf("must be a MessageAttributeValue object")
	}
	dataType, err := requiredString(object, "DataType")
	if err != nil {
		return sqsMessageAttribute{}, err
	}
	attribute := sqsMessageAttribute{DataType: dataType}
	if stringValue, ok, err := optionalString(object, "StringValue"); err != nil {
		return sqsMessageAttribute{}, err
	} else if ok {
		attribute.StringValue = stringValue
		attribute.HasStringValue = true
	}
	if binaryValue, ok, err := optionalBase64(object, "BinaryValue"); err != nil {
		return sqsMessageAttribute{}, err
	} else if ok {
		attribute.BinaryValue = binaryValue
		attribute.HasBinaryValue = true
	}
	if _, ok := object["StringListValues"]; ok {
		return sqsMessageAttribute{}, fmt.Errorf("StringListValues is reserved by SQS and not supported")
	}
	if _, ok := object["BinaryListValues"]; ok {
		return sqsMessageAttribute{}, fmt.Errorf("BinaryListValues is reserved by SQS and not supported")
	}
	return attribute, nil
}

func requiredString(object map[string]any, key string) (string, error) {
	value, ok, err := optionalString(object, key)
	if err != nil {
		return "", err
	}
	if !ok || value == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return value, nil
}

func optionalString(object map[string]any, key string) (string, bool, error) {
	value, ok := object[key]
	if !ok || value == nil {
		return "", false, nil
	}
	stringValue, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("%s must be a string", key)
	}
	return stringValue, true, nil
}

func optionalBase64(object map[string]any, key string) ([]byte, bool, error) {
	value, ok, err := optionalString(object, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, true, fmt.Errorf("%s must be base64: %w", key, err)
	}
	return decoded, true, nil
}

func sqsDelaySeconds(options backendOptions) (*int32, error) {
	value, ok := options.values["delaySeconds"]
	if !ok || value == nil {
		return nil, nil
	}
	var seconds int32
	switch typed := value.(type) {
	case int:
		seconds = int32(typed)
		if int(seconds) != typed {
			return nil, fmt.Errorf("%w: delaySeconds must be a valid int32", errMalformedOptions)
		}
	case int32:
		seconds = typed
	case int64:
		seconds = int32(typed)
		if int64(seconds) != typed {
			return nil, fmt.Errorf("%w: delaySeconds must be a valid int32", errMalformedOptions)
		}
	case float64:
		seconds = int32(typed)
		if float64(seconds) != typed {
			return nil, fmt.Errorf("%w: delaySeconds must be an integer", errMalformedOptions)
		}
	default:
		return nil, fmt.Errorf("%w: delaySeconds must be an integer", errMalformedOptions)
	}
	return &seconds, nil
}

func sqsAWSTraceHeader(options backendOptions) (string, error) {
	value, ok := options.values["messageSystemAttributes"]
	if !ok || value == nil {
		return "", nil
	}
	attributes, ok := normalizeObject(value)
	if !ok {
		return "", fmt.Errorf("%w: messageSystemAttributes must be an object", errMalformedOptions)
	}
	if len(attributes) == 0 {
		return "", nil
	}
	if len(attributes) > 1 {
		return "", fmt.Errorf("%w: only AWSTraceHeader is supported in messageSystemAttributes", errMalformedOptions)
	}
	if _, ok := attributes["AWSTraceHeader"]; !ok {
		return "", fmt.Errorf("%w: only AWSTraceHeader is supported in messageSystemAttributes", errMalformedOptions)
	}
	traceHeader, err := requiredString(attributes, "AWSTraceHeader")
	if err != nil {
		return "", fmt.Errorf("%w: %w", errMalformedOptions, err)
	}
	return traceHeader, nil
}

func validSQSAttributes(attributes map[string]sqsMessageAttribute) bool {
	if len(attributes) > sqsMaxAttributes {
		return false
	}
	for key, attribute := range attributes {
		if key == "" || attribute.DataType == "" {
			return false
		}
		if !sqsAttributeNameRe.MatchString(key) {
			return false
		}
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") {
			return false
		}
		if strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") || strings.Contains(key, "..") {
			return false
		}
		if !validSQSAttributeDataType(attribute.DataType) {
			return false
		}
		if attribute.HasStringList || attribute.HasBinaryList {
			return false
		}
		if strings.HasPrefix(attribute.DataType, "Binary") {
			if !attribute.HasBinaryValue || attribute.HasStringValue {
				return false
			}
		} else {
			if !attribute.HasStringValue || attribute.HasBinaryValue {
				return false
			}
			if attribute.StringValue == "" {
				return false
			}
			if !sqsAllowedUnicodeBytes([]byte(attribute.StringValue)) {
				return false
			}
		}
		if attribute.HasBinaryValue && len(attribute.BinaryValue) == 0 {
			return false
		}
	}
	return true
}

func validSQSAttributeDataType(dataType string) bool {
	base, _, _ := strings.Cut(dataType, ".")
	return base == "String" || base == "Number" || base == "Binary"
}

func sqsAllowedUnicodeBytes(value []byte) bool {
	if !utf8.Valid(value) {
		return false
	}
	for _, r := range string(value) {
		if r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		if r >= 0x20 && r <= 0xD7FF {
			continue
		}
		if r >= 0xE000 && r <= 0xFFFD {
			continue
		}
		if r >= 0x10000 && r <= 0x10FFFF {
			continue
		}
		return false
	}
	return true
}

func syntheticFIFOGroupID(eventID string) string {
	return "outboxer-" + stableDigest(eventID)
}

func providerSafeID(value string, pattern *regexp.Regexp) string {
	if pattern.MatchString(value) {
		return value
	}
	return stableDigest(value)
}

func stableDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func validSQSQueueURL(queueURL string) bool {
	if queueURL == "" || strings.ContainsAny(queueURL, " \t\r\n") {
		return false
	}

	parsed, err := url.Parse(queueURL)
	if err != nil {
		return false
	}
	if parsed.Scheme == "" {
		return true
	}
	return (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && strings.Trim(parsed.Path, "/") != ""
}

func isSQSPermanentRequestError(err error) bool {
	var apiErr interface{ ErrorCode() string }
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "InvalidMessageContents", "BatchRequestTooLong", "InvalidBatchEntryId", "BatchEntryIdsNotDistinct", "TooManyEntriesInBatchRequest":
		return true
	default:
		return false
	}
}
