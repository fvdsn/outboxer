package outboxer

import (
	"context"
	"database/sql"
	"log/slog"
	"math"
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

func init() {
	deadlockDetector.Store(randomInt63())
}

type batchResult struct {
	selected int
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
			sleepContext(ctx, a.cfg.ErrorCooldown)
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
		return batchResult{}, err
	}

	result, batchErr := a.processEventBatch(ctx, tx)
	if batchErr != nil {
		logBatchError(ctx, "Failed during batch transaction", batchErr)
	}

	if err := tx.Commit(); err != nil {
		logBatchError(ctx, "Failed to commit batch transaction", err)
		return result, err
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
	deadlockDetector.Store(randomInt63())

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		return batchResult{}, err
	}
	result := batchResult{selected: len(events)}
	if len(events) > 0 {
		slog.Info("Processing batch", "count", len(events))
	}

	var idsMu sync.Mutex
	idsToDelete := []any{}
	addIDToDelete := func(id any) {
		idsMu.Lock()
		defer idsMu.Unlock()
		idsToDelete = append(idsToDelete, id)
	}

	jobs := parallelizeEvents(a.cfg, events)
	errs := make(chan error, len(jobs))
	var wg sync.WaitGroup

	for _, jobEvents := range jobs {
		jobEvents := jobEvents
		wg.Add(1)
		go func() {
			defer wg.Done()

			pubsubEvents := []event{}
			sqsEvents := []event{}
			for _, evt := range jobEvents {
				switch a.resolveBackend(evt) {
				case backendPubSub:
					pubsubEvents = append(pubsubEvents, evt)
				case backendSQS:
					sqsEvents = append(sqsEvents, evt)
				default:
					slog.Error("Event has no enabled backend for its target, leaving it in the table",
						"event_id", eventValue(evt, a.cfg.EventID),
						"event_target", eventOptionalString(evt, a.cfg.EventTarget),
						"pubsub_enabled", a.cfg.PubSubEnabled,
						"sqs_enabled", a.cfg.SQSEnabled,
					)
				}
			}

			if err := a.sendPubsubEvents(ctx, tx, pubsubEvents, addIDToDelete); err != nil {
				errs <- err
				return
			}
			if err := a.sendSQSEvents(ctx, tx, sqsEvents, addIDToDelete); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	idsMu.Lock()
	deleteIDs := append([]any(nil), idsToDelete...)
	idsMu.Unlock()

	if err := a.deleteEvents(ctx, tx, deleteIDs); err != nil {
		return result, err
	}

	for err := range errs {
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

type backend int

const (
	backendNone backend = iota
	backendPubSub
	backendSQS
)

// resolveBackend decides where an event should be published based on the
// enabled backends and the event's target column. When only one backend is
// enabled the target is optional and every event routes there. When both are
// enabled the target must explicitly name a backend. Anything that cannot be
// routed returns backendNone so the caller can leave the row in place.
func (a *app) resolveBackend(evt event) backend {
	switch eventOptionalString(evt, a.cfg.EventTarget) {
	case eventTargetPubSub:
		if a.cfg.PubSubEnabled {
			return backendPubSub
		}
	case eventTargetSQS:
		if a.cfg.SQSEnabled {
			return backendSQS
		}
	case "":
		if a.cfg.PubSubEnabled && !a.cfg.SQSEnabled {
			return backendPubSub
		}
		if a.cfg.SQSEnabled && !a.cfg.PubSubEnabled {
			return backendSQS
		}
	}
	return backendNone
}

func parallelizeEvents(cfg appConfig, events []event) [][]event {
	jobs := make([][]event, cfg.BatchWorkers)
	seed := int(randomInt63() % 100000)

	for _, evt := range events {
		orderingKey := eventOptionalString(evt, cfg.EventOrderingKey)
		if orderingKey != "" {
			jobIdx := strHash(seed, orderingKey) % cfg.BatchWorkers
			if len(jobs[jobIdx]) >= cfg.BatchMaxSequential {
				continue
			}
			jobs[jobIdx] = append(jobs[jobIdx], evt)
			continue
		}

		jobIdx := 0
		for i := range jobs {
			if len(jobs[i]) < len(jobs[jobIdx]) {
				jobIdx = i
			}
		}
		jobs[jobIdx] = append(jobs[jobIdx], evt)
	}

	return jobs
}

func strHash(seed int, str string) int {
	hash := int32(seed)
	for _, char := range str {
		hash = (hash << 5) - hash + char
	}
	if hash == math.MinInt32 {
		return math.MaxInt32
	}
	if hash < 0 {
		return int(-hash)
	}
	return int(hash)
}

func randomInt63() int64 {
	randomMu.Lock()
	defer randomMu.Unlock()
	return randomSource.Int63()
}
