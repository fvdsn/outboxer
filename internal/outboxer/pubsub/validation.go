package pubsub

import (
	"errors"
	"regexp"
	"strings"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	pubsubMaxMessageDataBytes       = 10_000_000
	pubsubMaxPublishRequestMessages = 1000
	pubsubMaxAttributes             = 100
	pubsubMaxAttributeKeyBytes      = 256
	pubsubMaxAttributeValueBytes    = 1024
	pubsubPermanentBackendErrorCode = 400
)

var pubsubTopicIDPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$`)

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

func pubsubPoisonReason(message Message) (string, bool) {
	if len(message.Data) > pubsubMaxMessageDataBytes {
		return "Pub/Sub message data exceeds 10 MB", true
	}
	if len(message.Data) == 0 && len(message.Attributes) == 0 && message.OrderingKey == "" {
		return "Pub/Sub message has no data, attributes, or ordering key", true
	}
	if !validPubSubAttributes(message.Attributes) {
		return "Pub/Sub attributes exceed provider limits", true
	}
	if !validPubSubTopic(message.Topic) {
		return "Pub/Sub topic name is syntactically invalid", true
	}
	return "", false
}

func validPubSubAttributes(attributes map[string]string) bool {
	if len(attributes) > pubsubMaxAttributes {
		return false
	}
	for key, value := range attributes {
		if key == "" {
			return false
		}
		if len([]byte(key)) > pubsubMaxAttributeKeyBytes || len([]byte(value)) > pubsubMaxAttributeValueBytes {
			return false
		}
		if strings.HasPrefix(strings.ToLower(key), "goog") {
			return false
		}
	}
	return true
}

func validPubSubTopic(topic string) bool {
	parts := strings.Split(topic, "/")
	if len(parts) == 4 {
		return parts[0] == "projects" && parts[1] != "" && parts[2] == "topics" && validPubSubTopicID(parts[3])
	}
	if strings.Contains(topic, "/") {
		return false
	}
	return validPubSubTopicID(topic)
}

func validPubSubTopicID(topicID string) bool {
	return !strings.HasPrefix(strings.ToLower(topicID), "goog") && pubsubTopicIDPattern.MatchString(topicID)
}

func isPubSubPermanentBackendError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == pubsubPermanentBackendErrorCode {
		return true
	}

	code := status.Code(err)
	return code == codes.InvalidArgument || code == codes.OutOfRange
}
