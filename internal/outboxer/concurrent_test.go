package outboxer

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestRunConcurrentRunsEveryItemAndJoinsErrors(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")
	var calls atomic.Int32

	err := runConcurrent([]error{firstErr, nil, secondErr}, func(item error) error {
		calls.Add(1)
		return item
	})

	if calls.Load() != 3 {
		t.Fatalf("runConcurrent made %d calls, want 3", calls.Load())
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("runConcurrent returned %v, want both errors", err)
	}
}
