package sqs

import (
	"context"
	"fmt"
	"net/url"

	"cloud.google.com/go/compute/metadata"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type awsPublisher struct {
	client *sqs.Client
}

// NewClient creates a configured AWS SQS client.
func NewClient(ctx context.Context, cfg Config) (*sqs.Client, error) {
	loadOptions := []func(*config.LoadOptions) error{}
	if cfg.AWSRegion != "" {
		loadOptions = append(loadOptions, config.WithRegion(cfg.AWSRegion))
	}

	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, err
	}

	switch {
	case cfg.AWSWebIdentityProvider == WebIdentityProviderGoogle:
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

// NewPublisher creates an SQS publisher backed by the AWS SDK.
func NewPublisher(client *sqs.Client) Publisher {
	return &awsPublisher{client: client}
}

// googleIDTokenRetriever fetches a Google-signed OIDC ID token from the GCP
// metadata server for use as an AWS web identity token.
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

func (p *awsPublisher) SendBatch(ctx context.Context, queueURL string, entries []BatchEntry) (BatchResponse, error) {
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
		return BatchResponse{}, err
	}

	converted := BatchResponse{}
	for _, entry := range response.Successful {
		converted.Successful = append(converted.Successful, BatchSuccess{
			ID:        aws.ToString(entry.Id),
			MessageID: aws.ToString(entry.MessageId),
		})
	}
	for _, entry := range response.Failed {
		converted.Failed = append(converted.Failed, BatchFailure{
			ID:          aws.ToString(entry.Id),
			Code:        aws.ToString(entry.Code),
			Message:     aws.ToString(entry.Message),
			SenderFault: entry.SenderFault,
		})
	}
	return converted, nil
}

func convertAttributesToAWSSQS(attributes map[string]MessageAttribute) map[string]sqstypes.MessageAttributeValue {
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
