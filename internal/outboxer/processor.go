package outboxer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	randomMu     sync.Mutex
	randomSource = rand.New(rand.NewSource(time.Now().UnixNano()))

	// deadlockDetector is bumped to a fresh random value on every batch. The
	// watchdog goroutine reads it on a ticker; if it has not changed between two
	// ticks the process is assumed stuck and exits. It is accessed from both the
	// processing and watchdog goroutines, so it must be atomic.
	deadlockDetector atomic.Int64
)

var (
	errDatabaseBatch    = errors.New("database batch error")
	errFatalAfterCommit = errors.New("fatal after commit")
)

func init() {
	markProcessorProgress()
}

type batchResult struct {
	selected int
}

type sender interface {
	Send(ctx context.Context, events []event) (done []event, err error)
	Close() error
}

type appSender struct {
	send  func(ctx context.Context, events []event) ([]event, error)
	close func() error
}

func (s appSender) Send(ctx context.Context, events []event) ([]event, error) {
	return s.send(ctx, events)
}

func (s appSender) Close() error {
	if s.close == nil {
		return nil
	}
	return s.close()
}

type senderResult struct {
	done []event
	err  error
}

func startDeadlockDetector(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var previous int64
		for range ticker.C {
			current := deadlockDetector.Load()
			if current == previous {
				slog.Error("Deadlock detected, shutting down")
				os.Exit(1)
			}
			previous = current
			slog.Debug("Watchdog heartbeat")
		}
	}()
}

func (a *app) processEvents(ctx context.Context) {
	slog.Info("Processing events", "table", a.cfg.EventTable)

	for {
		if ctx.Err() != nil {
			return
		}

		result, err := a.processOneBatch(ctx)
		if err != nil {
			if errors.Is(err, errFatalAfterCommit) {
				slog.Error("Fatal sender error after commit, stopping processor", "error", err.Error())
				return
			}
			if errors.Is(err, errDatabaseBatch) {
				sleepContext(ctx, a.cfg.ErrorCooldown)
			}
		} else if result.selected == 0 && a.cfg.PollInterval > 0 {
			sleepContext(ctx, a.cfg.PollInterval)
		}
	}
}

// sleepContext waits for the given duration but returns early if the context is
// cancelled, so shutdown is not delayed by a cooldown or poll sleep.
func sleepContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (a *app) processOneBatch(ctx context.Context) (batchResult, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		logBatchError(ctx, "Failed to start batch transaction", err)
		return batchResult{}, fmtDBError(err)
	}

	result, batchErr := a.processEventBatch(ctx, tx)
	if batchErr != nil {
		logBatchError(ctx, "Failed during batch transaction", batchErr)
		if errors.Is(batchErr, errDatabaseBatch) {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				logBatchError(ctx, "Failed to rollback batch transaction", rollbackErr)
			}
			return result, batchErr
		}
	}

	if err := tx.Commit(); err != nil {
		logBatchError(ctx, "Failed to commit batch transaction", err)
		if errors.Is(batchErr, errFatalAfterCommit) {
			return result, errors.Join(batchErr, fmtDBError(err))
		}
		return result, fmtDBError(err)
	}

	return result, batchErr
}

// logBatchError logs a batch failure, unless the context has been cancelled, in
// which case the error is just the expected fallout of shutting down.
func logBatchError(ctx context.Context, message string, err error) {
	if ctx.Err() != nil {
		return
	}
	slog.Error(message, "error", err.Error())
}

func (a *app) processEventBatch(ctx context.Context, tx *sql.Tx) (batchResult, error) {
	markProcessorProgress()

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		return batchResult{}, fmtDBError(err)
	}
	result := batchResult{selected: len(events)}
	if len(events) > 0 {
		slog.Info("Processing batch", "count", len(events))
	}

	pubsubEvents := []event{}
	sqsEvents := []event{}
	for _, evt := range events {
		route := a.classifyRoute(evt)
		markProcessorProgress()
		switch route.backend {
		case backendPubSub:
			pubsubEvents = append(pubsubEvents, evt)
		case backendSQS:
			sqsEvents = append(sqsEvents, evt)
		default:
			a.logFailure(ctx, "Event cannot be routed, leaving it in the table",
				fmt.Sprintf("route|%s|%s|pubsub=%t|sqs=%t", route.failure, eventOptionalString(evt, a.cfg.EventTarget), a.cfg.PubSubEnabled, a.cfg.SQSEnabled),
				"event_id", eventValue(evt, a.cfg.EventID),
				"event_target", eventOptionalString(evt, a.cfg.EventTarget),
				"routing_failure", route.failure,
				"pubsub_enabled", a.cfg.PubSubEnabled,
				"sqs_enabled", a.cfg.SQSEnabled,
			)
		}
	}

	results := make(chan senderResult, 2)
	var wg sync.WaitGroup

	if len(pubsubEvents) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done, err := a.pubsubSender().Send(ctx, pubsubEvents)
			results <- senderResult{done: done, err: err}
		}()
	}

	if len(sqsEvents) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done, err := a.sqsSender().Send(ctx, sqsEvents)
			results <- senderResult{done: done, err: err}
		}()
	}

	wg.Wait()
	close(results)

	deleteIDs := []any{}
	var senderErr error
	for result := range results {
		for _, evt := range result.done {
			deleteIDs = append(deleteIDs, eventValue(evt, a.cfg.EventID))
		}
		if result.err != nil {
			senderErr = errors.Join(senderErr, result.err)
		}
	}

	if err := a.deleteEvents(ctx, tx, deleteIDs); err != nil {
		if senderErr != nil {
			return result, errors.Join(senderErr, fmtDBError(err))
		}
		return result, fmtDBError(err)
	}

	if senderErr != nil {
		return result, senderErr
	}

	return result, nil
}

func (a *app) pubsubSender() sender {
	return appSender{
		send: func(ctx context.Context, events []event) ([]event, error) {
			return a.collectSenderDoneEvents(ctx, events, func(addDoneID func(any)) error {
				return a.sendPubsubEvents(ctx, events, addDoneID)
			})
		},
		close: func() error {
			if a.pubsub == nil {
				return nil
			}
			return a.pubsub.Close()
		},
	}
}

func (a *app) sqsSender() sender {
	return appSender{
		send: func(ctx context.Context, events []event) ([]event, error) {
			return a.collectSenderDoneEvents(ctx, events, func(addDoneID func(any)) error {
				return a.sendSQSEvents(ctx, events, addDoneID)
			})
		},
	}
}

func (a *app) collectSenderDoneEvents(ctx context.Context, events []event, send func(addDoneID func(any)) error) ([]event, error) {
	eventsByID := map[string]event{}
	for _, evt := range events {
		eventsByID[eventIDKey(eventValue(evt, a.cfg.EventID))] = evt
	}

	seen := map[string]struct{}{}
	done := []event{}
	var doneMu sync.Mutex
	addDoneID := func(id any) {
		markProcessorProgress()
		key := eventIDKey(id)

		doneMu.Lock()
		defer doneMu.Unlock()
		evt, ok := eventsByID[key]
		if !ok {
			a.logFailure(ctx, "Sender reported an ID outside the selected batch, ignoring it",
				fmt.Sprintf("sender-outside-selection|%s", key),
				"event_id", id,
			)
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		done = append(done, evt)
	}

	err := send(addDoneID)
	doneMu.Lock()
	copiedDone := append([]event(nil), done...)
	doneMu.Unlock()
	return copiedDone, err
}

func fmtDBError(err error) error {
	return errors.Join(errDatabaseBatch, err)
}

func eventIDKey(id any) string {
	return fmt.Sprintf("%T:%v", id, id)
}

type backend int

const (
	backendNone backend = iota
	backendPubSub
	backendSQS
)

type routingFailure string

const (
	routingFailureNone          routingFailure = ""
	routingFailureDisabled      routingFailure = "R7"
	routingFailureUnsupported   routingFailure = "R10"
	routingFailureAmbiguous     routingFailure = "R11"
	routingFailureNoDestination routingFailure = "R12"
)

type routeResult struct {
	backend backend
	failure routingFailure
}

func (a *app) classifyRoute(evt event) routeResult {
	target := eventOptionalString(evt, a.cfg.EventTarget)
	switch target {
	case eventTargetPubSub:
		if a.cfg.PubSubEnabled {
			return a.routeToBackend(evt, backendPubSub)
		}
		return routeResult{failure: routingFailureDisabled}
	case eventTargetSQS:
		if a.cfg.SQSEnabled {
			return a.routeToBackend(evt, backendSQS)
		}
		return routeResult{failure: routingFailureDisabled}
	case "":
		if a.cfg.PubSubEnabled && !a.cfg.SQSEnabled {
			return a.routeToBackend(evt, backendPubSub)
		}
		if a.cfg.SQSEnabled && !a.cfg.PubSubEnabled {
			return a.routeToBackend(evt, backendSQS)
		}
		return routeResult{failure: routingFailureAmbiguous}
	}
	return routeResult{failure: routingFailureUnsupported}
}

func (a *app) routeToBackend(evt event, selected backend) routeResult {
	if a.destinationForBackend(evt, selected) == "" {
		return routeResult{failure: routingFailureNoDestination}
	}
	return routeResult{backend: selected}
}

func (a *app) destinationForBackend(evt event, selected backend) string {
	destination := eventString(evt, a.cfg.EventDestination)
	if destination != "" {
		return destination
	}
	switch selected {
	case backendPubSub:
		return a.cfg.DefaultPubSubTopic
	case backendSQS:
		return a.cfg.DefaultSQSQueueURL
	default:
		return ""
	}
}

// resolveBackend is kept as a small compatibility shim for existing tests and
// call sites that only need the selected backend.
func (a *app) resolveBackend(evt event) backend {
	return a.classifyRoute(evt).backend
}

func randomInt63() int64 {
	randomMu.Lock()
	defer randomMu.Unlock()
	return randomSource.Int63()
}

func markProcessorProgress() {
	deadlockDetector.Store(randomInt63())
}
