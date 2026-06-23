package outboxer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type poisonEvent struct {
	evt   event
	error string
}

func (a *app) checkDLQWorks(ctx context.Context) error {
	if a.cfg.DLQTable == "" {
		return nil
	}

	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf("SELECT %s, %s::jsonb FROM %s LIMIT 1", ident("id"), ident("event"), ident(a.cfg.DLQTable))
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (a *app) insertDeadLetters(ctx context.Context, tx *sql.Tx, poison []poisonEvent) error {
	if a.cfg.DLQTable == "" || len(poison) == 0 {
		return nil
	}

	ctx, cancel := withTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1::jsonb)", ident(a.cfg.DLQTable), ident("event"))
	for _, poisoned := range poison {
		payload, err := json.Marshal(a.deadLetterPayload(poisoned))
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, string(payload)); err != nil {
			return err
		}
		markProcessorProgress()
	}
	return nil
}

func (a *app) deadLetterPayload(poisoned poisonEvent) map[string]any {
	route := a.classifyRoute(poisoned.evt)
	target := ""
	destination := ""
	switch route.backend {
	case backendPubSub:
		target = eventTargetPubSub
		destination = a.destinationForBackend(poisoned.evt, backendPubSub)
	case backendSQS:
		target = eventTargetSQS
		destination = a.destinationForBackend(poisoned.evt, backendSQS)
	}

	payload := map[string]any{
		"source_table":     a.cfg.EventTable,
		"dead_lettered_at": time.Now().UTC().Format(time.RFC3339Nano),
		"target":           target,
		"destination":      destination,
		"original_event":   originalEventJSON(poisoned.evt),
	}
	if poisoned.error != "" {
		payload["error"] = poisoned.error
	}
	return payload
}

func originalEventJSON(evt event) map[string]any {
	out := make(map[string]any, len(evt.columns))
	for key, value := range evt.columns {
		out[key] = eventJSONValue(value)
	}
	return out
}

func eventJSONValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		var decoded any
		if json.Unmarshal(typed, &decoded) == nil {
			return decoded
		}
		return string(typed)
	default:
		return typed
	}
}
