package outboxer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

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
	sqsEventMaxSizeByte = 256 * 1024

	awsWebIdentityProviderGoogle = "google"
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

	return sqs.NewFromConfig(awsConfig), nil
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

func (a *app) sendSQSEvents(ctx context.Context, tx *sql.Tx, events []event, addIDToDelete func(any)) error {
	eventsByQueue := map[string][]event{}
	for _, evt := range events {
		queue := eventString(evt, a.cfg.EventDestination)
		if queue == "" {
			queue = a.cfg.DefaultSQSQueueURL
		}
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

	isFIFO := strings.HasSuffix(queueURL, ".fifo")

	start := time.Now()
	entries := []sqsBatchEntry{}
	idsByEntryID := map[string]any{}

	for _, evt := range events {
		orderingKey := eventOptionalString(evt, a.cfg.EventOrderingKey)
		attributes := eventAttributes(evt, a.cfg.EventAttributes)
		timestamp := eventValue(evt, a.cfg.EventTimestamp)
		id := eventValue(evt, a.cfg.EventID)
		entryID := fmt.Sprint(id)
		data := eventBytes(evt, a.cfg.EventPayload)
		latency := eventLatency(timestamp)

		if len(data) >= sqsEventMaxSizeByte {
			a.txMu.Lock()
			err := a.deleteEvent(ctx, tx, id)
			a.txMu.Unlock()
			if err != nil {
				return err
			}

			slog.Error("Failed to send event",
				"event_id", id,
				"event_destination", queueURL,
				"error", fmt.Sprintf("Event too big: %d bytes", len(data)),
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

		stringAttributes, deletedAttributes := sanitizeStringAttributes(attributes)
		if len(deletedAttributes) != 0 {
			slog.Error("Some attributes were dropped",
				"event_id", id,
				"event_destination", queueURL,
				"dropped_attributes", deletedAttributes,
			)
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
			entry.DeduplicationID = entryID
		}

		entries = append(entries, entry)
		idsByEntryID[entryID] = id
	}

	if len(entries) == 0 {
		return nil
	}

	sendCtx, cancel := withTimeout(ctx, a.cfg.PublishTimeout)
	defer cancel()
	response, err := a.sqs.SendBatch(sendCtx, queueURL, entries)
	if err != nil {
		slog.Error("Failed to send event batch",
			"event_destination", queueURL,
			"error", err.Error(),
		)
		return err
	}

	publishLatency := time.Since(start).Seconds()
	for _, entry := range response.Successful {
		originalID := idsByEntryID[entry.ID]
		addIDToDelete(originalID)
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
		}
		slog.Error("Failed to send event",
			"event_id", entry.ID,
			"event_destination", queueURL,
			"error", fmt.Sprintf("%s: %s", entry.Code, entry.Message),
		)
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
