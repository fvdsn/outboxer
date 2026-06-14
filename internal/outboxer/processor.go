package outboxer

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

var (
	randomMu                 sync.Mutex
	randomSource             = rand.New(rand.NewSource(time.Now().UnixNano()))
	deadlockDetector         = randomInt63()
	deadlockDetectorPrevious int64
)

func startDeadlockDetector(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if deadlockDetector == deadlockDetectorPrevious {
				logError(map[string]any{"message": "deadlock detected, shutting down"})
				os.Exit(1)
			}
			deadlockDetectorPrevious = deadlockDetector
			logInfo(map[string]any{"message": "all good"})
		}
	}()
}

func (a *app) processEvents(ctx context.Context, mode string) error {
	logInfo(map[string]any{"message": fmt.Sprintf("Processing events from table '%s'", a.cfg.EventTable)})

	for {
		tx, err := a.db.BeginTx(ctx, nil)
		if err != nil {
			logError(map[string]any{"message": "Error while starting event batch transaction", "error": err.Error()})
			if a.cfg.RunMode == runModeOnDemand {
				return err
			}
			time.Sleep(a.cfg.ErrorCooldown)
		} else {
			if err := a.processEventBatch(ctx, tx); err != nil {
				logError(map[string]any{"message": "Error during event batch transaction", "error": err.Error()})
				time.Sleep(a.cfg.ErrorCooldown)
			}
			if err := tx.Commit(); err != nil {
				logError(map[string]any{"message": "Error while starting event batch transaction", "error": err.Error()})
				if a.cfg.RunMode == runModeOnDemand {
					return err
				}
				time.Sleep(a.cfg.ErrorCooldown)
			}
		}

		if mode != runModePoll {
			break
		}
	}

	return nil
}

func (a *app) processEventBatch(ctx context.Context, tx *sql.Tx) error {
	deadlockDetector = randomInt63()

	events, err := a.selectEvents(ctx, tx)
	if err != nil {
		return err
	}
	if len(events) > 0 {
		logInfo(map[string]any{"message": fmt.Sprintf("processing %d messages", len(events))})
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
				if eventString(evt, a.cfg.EventTarget) == eventTargetSQS {
					sqsEvents = append(sqsEvents, evt)
				} else {
					pubsubEvents = append(pubsubEvents, evt)
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
		return err
	}

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
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
