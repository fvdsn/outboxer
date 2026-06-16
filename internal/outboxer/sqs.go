package outboxer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
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
	ID              string
	MessageBody     string
	Attributes      map[string]string
	MessageGroupID  string
	DeduplicationID string
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
		if entry.MessageGroupID != "" {
			awsEntry.MessageGroupId = aws.String(entry.MessageGroupID)
		}
		if entry.DeduplicationID != "" {
			awsEntry.MessageDeduplicationId = aws.String(entry.DeduplicationID)
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

func (a *app) sendSQSEvents(ctx context.Context, events []event, addIDToDelete func(any)) error {
	eventsByQueue := map[string][]event{}
	for _, evt := range events {
		queue := a.destinationForBackend(evt, backendSQS)
		eventsByQueue[queue] = append(eventsByQueue[queue], evt)
	}

	sem := make(chan struct{}, a.cfg.SQSSendConcurrency)
	errs := make(chan error, len(eventsByQueue))
	var wg sync.WaitGroup
	for queue, queueEvents := range eventsByQueue {
		queue := queue
		queueEvents := append([]event(nil), queueEvents...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if strings.HasSuffix(queue, ".fifo") {
				err = a.sendSQSFIFOEvents(ctx, sem, queue, queueEvents, addIDToDelete)
			} else {
				err = a.sendSQSStandardEvents(ctx, sem, queue, queueEvents, addIDToDelete)
			}
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (a *app) sendSQSStandardEvents(ctx context.Context, sem chan struct{}, queue string, queueEvents []event, addIDToDelete func(any)) error {
	chunks := chunkSQSStandardEvents(queueEvents, a.cfg)
	errs := make(chan error, len(chunks))
	var wg sync.WaitGroup
	for _, chunk := range chunks {
		chunk := append([]event(nil), chunk...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := a.sendSQSBatchWithSemaphore(ctx, sem, queue, chunk, false, addIDToDelete); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func chunkSQSStandardEvents(events []event, cfg appConfig) [][]event {
	chunks := [][]event{}
	current := []event{}
	currentSize := 0

	for _, evt := range events {
		size := sqsEventMessageSize(evt, cfg)
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

func (a *app) sendSQSFIFOEvents(ctx context.Context, sem chan struct{}, queue string, queueEvents []event, addIDToDelete func(any)) error {
	groups := map[string][]event{}
	groupOrder := []string{}
	for _, evt := range queueEvents {
		groupID := sqsMessageGroupID(evt, a.cfg)
		if _, ok := groups[groupID]; !ok {
			groupOrder = append(groupOrder, groupID)
		}
		if len(groups[groupID]) < a.cfg.OrderedGroupBatchCap {
			groups[groupID] = append(groups[groupID], evt)
		}
	}

	errs := make(chan error, len(groupOrder))
	var wg sync.WaitGroup
	for _, groupID := range groupOrder {
		groupEvents := append([]event(nil), groups[groupID]...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, evt := range groupEvents {
				done, err := a.sendSQSBatchWithSemaphore(ctx, sem, queue, []event{evt}, true, addIDToDelete)
				if err != nil {
					errs <- err
					return
				}
				if !done {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (a *app) sendSQSBatchWithSemaphore(ctx context.Context, sem chan struct{}, queue string, events []event, isFIFO bool, addIDToDelete func(any)) (bool, error) {
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return a.sendSQSBatch(ctx, queue, events, isFIFO, addIDToDelete)
}

func (a *app) sendSQS10Events(ctx context.Context, queueURL string, events []event, addIDToDelete func(any)) error {
	_, err := a.sendSQSBatch(ctx, queueURL, events, strings.HasSuffix(queueURL, ".fifo"), addIDToDelete)
	return err
}

func (a *app) sendSQSBatch(ctx context.Context, queueURL string, events []event, isFIFO bool, addIDToDelete func(any)) (bool, error) {
	if len(events) == 0 {
		return false, nil
	}
	if !validSQSQueueURL(queueURL) {
		for _, evt := range events {
			addIDToDelete(eventValue(evt, a.cfg.EventID))
		}
		a.logFailure(ctx, "Failed to send event batch",
			fmt.Sprintf("sqs|%s|invalid-queue-url", queueURL),
			"event_destination", queueURL,
			"error", "SQS queue URL is syntactically invalid",
		)
		return true, nil
	}

	start := time.Now()
	entries := []sqsBatchEntry{}
	idsByEntryID := map[string]any{}

	for _, evt := range events {
		orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
		attributes := eventAttributes(evt, a.cfg.EventAttributes)
		timestamp := eventValue(evt, a.cfg.EventTimestamp)
		id := eventValue(evt, a.cfg.EventID)
		eventID := fmt.Sprint(id)
		entryID := providerSafeID(eventID, sqsBatchEntryIDPattern)
		data := eventBytes(evt, a.cfg.EventPayload)
		latency := eventLatency(timestamp)
		stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)

		if len(deletedAttributes) != 0 {
			slog.Error("Some attributes were dropped",
				"event_id", id,
				"event_destination", queueURL,
				"dropped_attributes", deletedAttributes,
			)
		}
		if isSQSPoison(data, stringAttributes, isFIFO, orderingKey) {
			addIDToDelete(id)
			a.logFailure(ctx, "Failed to send event",
				fmt.Sprintf("sqs|%s|%s|local-poison", queueURL, orderingKey),
				"event_id", id,
				"event_destination", queueURL,
				"error", "Event is invalid for SQS",
			)
			continue
		}

		slog.Debug("Sending event",
			"event_id", id,
			"event_timestamp", timestamp,
			"event_latency", latency,
			"event_payload_size", len(data),
			"event_ordering_key", orderingKey,
			"event_attributes", attributes,
			"event_target", eventTargetSQS,
			"event_destination", queueURL,
		)

		entry := sqsBatchEntry{
			ID:          entryID,
			MessageBody: string(data),
			Attributes:  stringAttributes,
		}
		if isFIFO {
			groupID := orderingKey
			if groupID == "" {
				groupID = syntheticFIFOGroupID(eventID)
			}
			entry.MessageGroupID = groupID
			entry.DeduplicationID = providerSafeID(eventID, sqsFIFOIDPattern)
		}

		entries = append(entries, entry)
		idsByEntryID[entryID] = id
	}

	if len(entries) == 0 {
		return true, nil
	}

	sendCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout)
	defer cancel()
	response, err := a.sqs.SendBatch(sendCtx, queueURL, entries)
	if err != nil {
		if isSQSPermanentRequestError(err) {
			if len(events) == 1 {
				addIDToDelete(eventValue(events[0], a.cfg.EventID))
				a.logFailure(ctx, "Failed to send event",
					fmt.Sprintf("sqs|%s|%s", queueURL, err.Error()),
					"event_id", eventValue(events[0], a.cfg.EventID),
					"event_destination", queueURL,
					"error", err.Error(),
				)
				return true, nil
			}
			return a.sendSQSBatchIsolated(ctx, queueURL, events, isFIFO, addIDToDelete)
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
		addIDToDelete(originalID)
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
			addIDToDelete(idsByEntryID[entry.ID])
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

func (a *app) sendSQSBatchIsolated(ctx context.Context, queueURL string, events []event, isFIFO bool, addIDToDelete func(any)) (bool, error) {
	anyDone := false
	var joined error
	for _, evt := range events {
		done, err := a.sendSQSBatch(ctx, queueURL, []event{evt}, isFIFO, addIDToDelete)
		if done {
			anyDone = true
		}
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return anyDone, joined
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

func isSQSPoison(body []byte, attributes map[string]string, isFIFO bool, orderingKey string) bool {
	if len(body) == 0 || !sqsAllowedUnicodeBytes(body) {
		return true
	}
	if sqsMessageSize(body, attributes) > sqsEventMaxSizeByte {
		return true
	}
	if !validSQSAttributes(attributes) {
		return true
	}
	if isFIFO && orderingKey != "" && !sqsFIFOIDPattern.MatchString(orderingKey) {
		return true
	}
	return false
}

func sqsMessageSize(body []byte, attributes map[string]string) int {
	size := len(body)
	for key, value := range attributes {
		size += len(key) + len("String") + len(value)
	}
	return size
}

func sqsEventMessageSize(evt event, cfg appConfig) int {
	stringAttributes, _ := sanitizeStringAttributes(eventAttributes(evt, cfg.EventAttributes))
	return sqsMessageSize(eventBytes(evt, cfg.EventPayload), stringAttributes)
}

func validSQSAttributes(attributes map[string]string) bool {
	if len(attributes) > sqsMaxAttributes {
		return false
	}
	for key, value := range attributes {
		if key == "" || value == "" {
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
		if !sqsAllowedUnicodeBytes([]byte(value)) {
			return false
		}
	}
	return true
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

func sqsMessageGroupID(evt event, cfg appConfig) string {
	eventID := fmt.Sprint(eventValue(evt, cfg.EventID))
	orderingKey := eventOptionalString(evt, cfg.EventOrderingKey)
	if orderingKey != "" {
		return orderingKey
	}
	return syntheticFIFOGroupID(eventID)
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
