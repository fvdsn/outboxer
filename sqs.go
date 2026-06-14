package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	eventTargetSQS      = "sqs"
	sqsEventBatchSize   = 10
	sqsEventMaxSizeByte = 256 * 1024
)

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
