package outboxer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

var (
	errDatabaseBatch    = errors.New("database batch error")
	errFatalAfterCommit = provider.ErrFatalAfterCommit
)

type batchResult struct {
	selected int
	// drained means every route the batch saw was fully collected, so the
	// events kept for retry are the relay's entire remaining backlog.
	drained bool
	stats   batchStats
}

type senderOutput struct {
	confirmed []event
	poison    []poisonEvent
}

type senderResult struct {
	output senderOutput
	err    error
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
		a.updateBacklog(ctx, result)

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
	a.markProgress()
	a.stats.addCommittedBatch(result.stats, time.Now())

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

// batchTriage is the dispatch plan for one selected batch: events grouped by
// their resolved target, plus the events that can never be sent as-is.
type batchTriage struct {
	eventsByTarget map[string][]event
	poison         []poisonEvent
}

// triageEvents partitions a selected batch into per-target dispatch groups and
// poison events, parsing each event's options once. It errors when a selected
// event violates a selection invariant (an unresolved route, or a target with
// no configured sender): those mean the selection query and the configuration
// disagree, so the whole batch must be rolled back rather than dispatched.
func (a *app) triageEvents(events []event) (batchTriage, error) {
	triage := batchTriage{eventsByTarget: map[string][]event{}}
	now := time.Now().UTC()
	for _, evt := range events {
		a.markProgress()
		if a.isExpiredEvent(evt, now) {
			triage.poison = append(triage.poison, poisonEvent{evt: evt, error: "Event expired by MAX_EVENT_AGE_MS"})
			continue
		}
		if evt.route.target == "" {
			return triage, fmt.Errorf("selected event %v has no resolved route", eventValue(evt, a.cfg.EventID))
		}
		if _, ok := a.senders[evt.route.target]; !ok {
			return triage, fmt.Errorf("selected event target %q has no configured sender", evt.route.target)
		}
		parsed, err := evt.withParsedOptions(a.cfg)
		if err != nil {
			triage.poison = append(triage.poison, poisonEvent{evt: evt, error: err.Error()})
			continue
		}
		triage.eventsByTarget[evt.route.target] = append(triage.eventsByTarget[evt.route.target], parsed)
	}
	return triage, nil
}

func (a *app) processEventBatch(ctx context.Context, tx *sql.Tx) (batchResult, error) {
	a.markProgress()

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		return batchResult{}, fmtDBError(err)
	}
	a.markProgress()
	result := batchResult{selected: len(events), drained: batchDrained(events, a.cfg.CollectBatchTarget)}
	result.stats.selected = len(events)
	result.stats.oldestEventAge = a.oldestEventAge(events)
	if len(events) > 0 {
		slog.Info("Processing batch", "count", len(events))
	}

	triage, err := a.triageEvents(events)
	if err != nil {
		return result, fmtDBError(err)
	}
	poisonEvents := triage.poison
	deleteIDs := make([]provider.EventID, 0, len(events))
	for _, poisoned := range poisonEvents {
		deleteIDs = append(deleteIDs, eventValue(poisoned.evt, a.cfg.EventID))
	}

	results := make(chan senderResult, len(triage.eventsByTarget))
	var wg sync.WaitGroup

	for target, targetEvents := range triage.eventsByTarget {
		sender := a.senders[target]
		events := targetEvents
		wg.Add(1)
		go func() {
			defer wg.Done()
			output, err := a.collectProviderOutput(ctx, sender, events)
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
	a.markProgress()

	if err := a.deleteEvents(ctx, tx, deleteIDs); err != nil {
		if senderErr != nil {
			return result, errors.Join(senderErr, fmtDBError(err))
		}
		return result, fmtDBError(err)
	}
	a.markProgress()

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

// oldestEventAge observes the outbox lag from the batch itself: events are
// selected in id order, so the first one is the oldest pending. It is 0 when
// the outbox was empty or the event carries no usable timestamp.
func (a *app) oldestEventAge(events []event) time.Duration {
	if len(events) == 0 || a.cfg.EventTimestamp == "" {
		return 0
	}
	timestamp, ok := eventTimestamp(eventValue(events[0], a.cfg.EventTimestamp))
	if !ok {
		return 0
	}
	return max(0, time.Since(timestamp))
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

func (a *app) collectProviderOutput(ctx context.Context, sender provider.Sender, events []event) (senderOutput, error) {
	return a.collectSenderOutput(ctx, events, func(callbacks provider.Callbacks) error {
		return sender.Send(ctx, providerEvents(events, a.cfg), callbacks)
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

func newSenderCollector(ctx context.Context, a *app, events []event) *senderCollector {
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

func (c *senderCollector) record(id provider.EventID, reason string, poisoned bool) {
	c.app.markProgress()
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

func (c *senderCollector) confirm(id provider.EventID) {
	c.record(id, "", false)
}

func (c *senderCollector) poison(id provider.EventID, reason string) {
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

func (a *app) collectSenderOutput(ctx context.Context, events []event, send func(provider.Callbacks) error) (senderOutput, error) {
	collector := newSenderCollector(ctx, a, events)
	err := send(provider.Callbacks{
		AddConfirmedID: collector.confirm,
		AddPoisonID:    collector.poison,
		MarkProgress:   a.markProgress,
		LogFailure:     a.logFailure,
	})
	return collector.snapshot(), err
}

func fmtDBError(err error) error {
	return errors.Join(errDatabaseBatch, err)
}

func eventIDKey(id provider.EventID) string {
	return fmt.Sprintf("%T:%v", id, id)
}
