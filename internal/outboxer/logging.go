package outboxer

import (
	"encoding/json"
	"fmt"
	"time"
)

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
