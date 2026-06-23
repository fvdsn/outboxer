package outboxer

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var errMalformedOptions = errors.New("malformed event options")

type event struct {
	columns map[string]any
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

func eventPubSubOptions(evt event, cfg appConfig) (backendOptions, error) {
	return eventBackendOptions(evt, cfg.EventOptions, eventTargetPubSub)
}

func eventSQSOptions(evt event, cfg appConfig) (backendOptions, error) {
	return eventBackendOptions(evt, cfg.EventOptions, eventTargetSQS)
}

func eventBackendOptions(evt event, column string, backend string) (backendOptions, error) {
	root, err := eventOptionsObject(evt, column)
	if err != nil {
		return backendOptions{}, err
	}
	value, ok := root[backend]
	if !ok || value == nil {
		return backendOptions{}, nil
	}
	section, ok := normalizeObject(value)
	if !ok {
		return backendOptions{}, fmt.Errorf("%w: %s section must be an object", errMalformedOptions, backend)
	}
	return backendOptions{values: section}, nil
}

func eventOptionsObject(evt event, column string) (map[string]any, error) {
	if column == "" {
		return nil, nil
	}
	value := eventValue(evt, column)
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		return typed, nil
	case map[string]string:
		return stringMapToAnyMap(typed), nil
	case []byte:
		return parseOptionsJSON(typed)
	case string:
		return parseOptionsJSON([]byte(typed))
	default:
		return nil, fmt.Errorf("%w: options column must be an object", errMalformedOptions)
	}
}

func parseOptionsJSON(content []byte) (map[string]any, error) {
	if len(content) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(content, &decoded); err != nil {
		return nil, fmt.Errorf("%w: %v", errMalformedOptions, err)
	}
	if decoded == nil {
		return nil, nil
	}
	options, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: options column must be an object", errMalformedOptions)
	}
	return options, nil
}

type backendOptions struct {
	values map[string]any
}

func (o backendOptions) stringValue(key string) (string, error) {
	value, ok := o.values[key]
	if !ok || value == nil {
		return "", nil
	}
	stringValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%w: %s must be a string", errMalformedOptions, key)
	}
	return stringValue, nil
}

func (o backendOptions) attributesValue(key string) (map[string]any, error) {
	value, ok := o.values[key]
	if !ok || value == nil {
		return nil, nil
	}
	attributes, ok := normalizeObject(value)
	if !ok {
		return nil, fmt.Errorf("%w: %s must be an object", errMalformedOptions, key)
	}
	return attributes, nil
}

func normalizeObject(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[string]string:
		return stringMapToAnyMap(typed), true
	default:
		return nil, false
	}
}

func stringMapToAnyMap(value map[string]string) map[string]any {
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
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
