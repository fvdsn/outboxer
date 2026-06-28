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

	outboxpubsub "github.com/fvdsn/outboxer/internal/outboxer/pubsub"
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
	errFatalAfterCommit = outboxpubsub.ErrFatalAfterCommit
)

func init() {
	markProcessorProgress()
}

type batchResult struct {
	selected int
	stats    batchStats
}

type senderOutput struct {
	confirmed []event
	poison    []poisonEvent
}

type senderResult struct {
	output senderOutput
	err    error
}

type senderCallbacks struct {
	addConfirmedID func(any)
	addPoisonID    func(any, string)
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

func (a *app) processEvents(ctx context.Context) error {
	slog.Info("Processing events", "table", a.cfg.EventTable)
	if a.cfg.PollInterval > 0 {
		slog.Debug("Notification wake-ups enabled", "channel", a.cfg.NotifyChannel)
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		result, err := a.processOneBatch(ctx)
		if err != nil {
			if errors.Is(err, errFatalAfterCommit) {
				slog.Error("Fatal sender error after commit, stopping processor", "error", err.Error())
				return err
			}
			if errors.Is(err, errDatabaseBatch) {
				sleepContext(ctx, a.cfg.ErrorCooldown)
			}
			continue
		}

		if result.selected == 0 && a.cfg.PollInterval > 0 {
			a.waitForEvents(ctx)
		}
	}
}

// waitForEvents waits for a new-event notification, bounded by the poll interval
// as a backstop. It listens on a transient connection borrowed for this idle
// cycle and released before returning, so it never holds a connection while
// batches run. If the listener cannot be established it falls back to a plain
// sleep; the next idle cycle tries again.
func (a *app) waitForEvents(ctx context.Context) {
	listener, err := a.startListener(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Debug("Failed to start notification listener, polling instead", "error", err.Error())
		}
		sleepContext(ctx, a.cfg.PollInterval)
		return
	}
	defer listener.close()

	if err := listener.wait(ctx, a.cfg.PollInterval); err != nil && ctx.Err() == nil {
		slog.Debug("Notification wait failed, polling next cycle", "error", err.Error())
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
		a.stats.addBatchError()
		return batchResult{}, fmtDBError(err)
	}

	result, batchErr := a.processEventBatch(ctx, tx)
	if batchErr != nil {
		logBatchError(ctx, "Failed during batch transaction", batchErr)
		if errors.Is(batchErr, errDatabaseBatch) {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				logBatchError(ctx, "Failed to rollback batch transaction", rollbackErr)
			}
			a.stats.addBatchError()
			return result, batchErr
		}
	}

	if err := tx.Commit(); err != nil {
		logBatchError(ctx, "Failed to commit batch transaction", err)
		a.stats.addBatchError()
		if errors.Is(batchErr, errFatalAfterCommit) {
			return result, errors.Join(batchErr, fmtDBError(err))
		}
		return result, fmtDBError(err)
	}
	markProcessorProgress()
	a.stats.addCommittedBatch(result.stats)

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
	markProcessorProgress()
	result := batchResult{selected: len(events)}
	result.stats.selected = len(events)
	if len(events) > 0 {
		slog.Info("Processing batch", "count", len(events))
	}

	pubsubEvents := []event{}
	sqsEvents := []event{}
	poisonEvents := []poisonEvent{}
	deleteIDs := []any{}
	for _, evt := range events {
		if a.isExpiredEvent(evt, time.Now().UTC()) {
			poisonEvents = append(poisonEvents, poisonEvent{evt: evt, error: "Event expired by MAX_EVENT_AGE_MS"})
			deleteIDs = append(deleteIDs, eventValue(evt, a.cfg.EventID))
			markProcessorProgress()
			continue
		}

		markProcessorProgress()
		switch evt.route.backend {
		case backendPubSub:
			pubsubEvents = append(pubsubEvents, evt)
		case backendSQS:
			sqsEvents = append(sqsEvents, evt)
		default:
			return result, fmtDBError(fmt.Errorf("selected event %v has no resolved route", eventValue(evt, a.cfg.EventID)))
		}
	}

	results := make(chan senderResult, 2)
	var wg sync.WaitGroup

	if len(pubsubEvents) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			output, err := a.collectPubsubOutput(ctx, pubsubEvents)
			results <- senderResult{output: output, err: err}
		}()
	}

	if len(sqsEvents) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			output, err := a.collectSQSOutput(ctx, sqsEvents)
			results <- senderResult{output: output, err: err}
		}()
	}

	wg.Wait()
	close(results)

	var senderErr error
	for senderResult := range results {
		for _, evt := range senderResult.output.confirmed {
			deleteIDs = append(deleteIDs, eventValue(evt, a.cfg.EventID))
			result.stats.sent++
		}
		for _, poisoned := range senderResult.output.poison {
			poisonEvents = append(poisonEvents, poisoned)
			deleteIDs = append(deleteIDs, eventValue(poisoned.evt, a.cfg.EventID))
		}
		if senderResult.err != nil {
			senderErr = errors.Join(senderErr, senderResult.err)
		}
	}
	result.stats.senderErrors = countJoinedErrors(senderErr)
	if errors.Is(senderErr, errFatalAfterCommit) {
		result.stats.fatalAfterCommitErrors = 1
	}
	if a.cfg.DLQTable == "" {
		result.stats.poison = len(poisonEvents)
	} else {
		result.stats.dlq = len(poisonEvents)
	}
	result.stats.keptForRetry = result.stats.selected - result.stats.sent - result.stats.poison - result.stats.dlq
	if result.stats.keptForRetry < 0 {
		result.stats.keptForRetry = 0
	}

	if err := a.insertDeadLetters(ctx, tx, poisonEvents); err != nil {
		if senderErr != nil {
			return result, errors.Join(senderErr, fmtDBError(err))
		}
		return result, fmtDBError(err)
	}
	markProcessorProgress()

	if err := a.deleteEvents(ctx, tx, deleteIDs); err != nil {
		if senderErr != nil {
			return result, errors.Join(senderErr, fmtDBError(err))
		}
		return result, fmtDBError(err)
	}
	markProcessorProgress()

	if senderErr != nil {
		return result, senderErr
	}

	return result, nil
}

func countJoinedErrors(err error) int {
	if err == nil {
		return 0
	}
	type unwrapper interface{ Unwrap() []error }
	if joined, ok := err.(unwrapper); ok {
		count := 0
		for _, item := range joined.Unwrap() {
			count += countJoinedErrors(item)
		}
		return count
	}
	return 1
}

func (a *app) isExpiredEvent(evt event, now time.Time) bool {
	if a.cfg.MaxEventAge <= 0 {
		return false
	}
	timestamp, ok := eventTimestamp(eventValue(evt, a.cfg.EventTimestamp))
	if !ok {
		return false
	}
	return now.Sub(timestamp) > a.cfg.MaxEventAge
}

func (a *app) collectPubsubOutput(ctx context.Context, events []event) (senderOutput, error) {
	return a.collectSenderOutput(ctx, events, func(callbacks senderCallbacks) error {
		return a.sendPubsubEventsWithCallbacks(ctx, events, callbacks)
	})
}

func (a *app) collectSQSOutput(ctx context.Context, events []event) (senderOutput, error) {
	return a.collectSenderOutput(ctx, events, func(callbacks senderCallbacks) error {
		return a.sendSQSEventsWithCallbacks(ctx, events, callbacks)
	})
}

type senderCollector struct {
	app        *app
	ctx        context.Context
	eventsByID map[string]event

	mu     sync.Mutex
	seen   map[string]struct{}
	output senderOutput
}

func newSenderCollector(a *app, ctx context.Context, events []event) *senderCollector {
	eventsByID := make(map[string]event, len(events))
	for _, evt := range events {
		eventsByID[eventIDKey(eventValue(evt, a.cfg.EventID))] = evt
	}
	return &senderCollector{
		app:        a,
		ctx:        ctx,
		eventsByID: eventsByID,
		seen:       map[string]struct{}{},
	}
}

func (c *senderCollector) record(id any, reason string, poisoned bool) {
	markProcessorProgress()
	key := eventIDKey(id)

	c.mu.Lock()
	evt, ok := c.eventsByID[key]
	if !ok {
		c.mu.Unlock()
		c.app.logFailure(c.ctx, "Sender reported an ID outside the selected batch, ignoring it",
			fmt.Sprintf("sender-outside-selection|%s", key),
			"event_id", id,
		)
		return
	}
	if _, ok := c.seen[key]; ok {
		c.mu.Unlock()
		return
	}
	c.seen[key] = struct{}{}
	if poisoned {
		c.output.poison = append(c.output.poison, poisonEvent{evt: evt, error: reason})
	} else {
		c.output.confirmed = append(c.output.confirmed, evt)
	}
	c.mu.Unlock()
}

func (c *senderCollector) confirm(id any) {
	c.record(id, "", false)
}

func (c *senderCollector) poison(id any, reason string) {
	c.record(id, reason, true)
}

func (c *senderCollector) snapshot() senderOutput {
	c.mu.Lock()
	defer c.mu.Unlock()
	return senderOutput{
		confirmed: append([]event(nil), c.output.confirmed...),
		poison:    append([]poisonEvent(nil), c.output.poison...),
	}
}

func (a *app) collectSenderOutput(ctx context.Context, events []event, send func(senderCallbacks) error) (senderOutput, error) {
	collector := newSenderCollector(a, ctx, events)
	err := send(senderCallbacks{
		addConfirmedID: collector.confirm,
		addPoisonID:    collector.poison,
	})
	return collector.snapshot(), err
}

func fmtDBError(err error) error {
	return errors.Join(errDatabaseBatch, err)
}

func eventIDKey(id any) string {
	return fmt.Sprintf("%T:%v", id, id)
}

func randomInt63() int64 {
	randomMu.Lock()
	defer randomMu.Unlock()
	return randomSource.Int63()
}

func markProcessorProgress() {
	deadlockDetector.Store(randomInt63())
}
