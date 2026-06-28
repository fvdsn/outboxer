package sqs

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

// MessageAttribute is the provider representation of an SQS message attribute.
type MessageAttribute struct {
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

func isSQSPoison(body []byte, attributes map[string]MessageAttribute, orderingKey string, deduplicationID string, delaySeconds *int32) bool {
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

func sqsMessageSize(body []byte, attributes map[string]MessageAttribute) int {
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

func sqsAttributes(options provider.Options) (map[string]MessageAttribute, error) {
	value, ok := options.Values["attributes"]
	if !ok || value == nil {
		return nil, nil
	}
	attributes, ok := provider.Object(value)
	if !ok {
		return nil, fmt.Errorf("%w: attributes must be an object", provider.ErrMalformedOptions)
	}
	converted := map[string]MessageAttribute{}
	for name, raw := range attributes {
		attr, err := sqsMessageAttributeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: attribute %s: %w", provider.ErrMalformedOptions, name, err)
		}
		converted[name] = attr
	}
	return converted, nil
}

func sqsMessageAttributeValue(value any) (MessageAttribute, error) {
	object, ok := provider.Object(value)
	if !ok {
		return MessageAttribute{}, fmt.Errorf("must be a MessageAttributeValue object")
	}
	dataType, err := requiredString(object, "DataType")
	if err != nil {
		return MessageAttribute{}, err
	}
	attribute := MessageAttribute{DataType: dataType}
	if stringValue, ok, err := optionalString(object, "StringValue"); err != nil {
		return MessageAttribute{}, err
	} else if ok {
		attribute.StringValue = stringValue
		attribute.HasStringValue = true
	}
	if binaryValue, ok, err := optionalBase64(object, "BinaryValue"); err != nil {
		return MessageAttribute{}, err
	} else if ok {
		attribute.BinaryValue = binaryValue
		attribute.HasBinaryValue = true
	}
	if _, ok := object["StringListValues"]; ok {
		return MessageAttribute{}, fmt.Errorf("StringListValues is reserved by SQS and not supported")
	}
	if _, ok := object["BinaryListValues"]; ok {
		return MessageAttribute{}, fmt.Errorf("BinaryListValues is reserved by SQS and not supported")
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

func sqsDelaySeconds(options provider.Options) (*int32, error) {
	value, ok := options.Values["delaySeconds"]
	if !ok || value == nil {
		return nil, nil
	}
	var seconds int32
	switch typed := value.(type) {
	case int:
		seconds = int32(typed)
		if int(seconds) != typed {
			return nil, fmt.Errorf("%w: delaySeconds must be a valid int32", provider.ErrMalformedOptions)
		}
	case int32:
		seconds = typed
	case int64:
		seconds = int32(typed)
		if int64(seconds) != typed {
			return nil, fmt.Errorf("%w: delaySeconds must be a valid int32", provider.ErrMalformedOptions)
		}
	case float64:
		seconds = int32(typed)
		if float64(seconds) != typed {
			return nil, fmt.Errorf("%w: delaySeconds must be an integer", provider.ErrMalformedOptions)
		}
	default:
		return nil, fmt.Errorf("%w: delaySeconds must be an integer", provider.ErrMalformedOptions)
	}
	return &seconds, nil
}

func sqsAWSTraceHeader(options provider.Options) (string, error) {
	value, ok := options.Values["messageSystemAttributes"]
	if !ok || value == nil {
		return "", nil
	}
	attributes, ok := provider.Object(value)
	if !ok {
		return "", fmt.Errorf("%w: messageSystemAttributes must be an object", provider.ErrMalformedOptions)
	}
	if len(attributes) == 0 {
		return "", nil
	}
	if len(attributes) > 1 {
		return "", fmt.Errorf("%w: only AWSTraceHeader is supported in messageSystemAttributes", provider.ErrMalformedOptions)
	}
	if _, ok := attributes["AWSTraceHeader"]; !ok {
		return "", fmt.Errorf("%w: only AWSTraceHeader is supported in messageSystemAttributes", provider.ErrMalformedOptions)
	}
	traceHeader, err := requiredString(attributes, "AWSTraceHeader")
	if err != nil {
		return "", fmt.Errorf("%w: %w", provider.ErrMalformedOptions, err)
	}
	return traceHeader, nil
}

func validSQSAttributes(attributes map[string]MessageAttribute) bool {
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
