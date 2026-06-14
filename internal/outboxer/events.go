package outboxer

import (
	"encoding/json"
	"fmt"
	"time"
)

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
