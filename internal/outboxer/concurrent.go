package outboxer

import (
	"errors"
	"sync"
)

// runConcurrent runs one independent operation per item and joins every error.
// It does not cancel sibling operations when one fails.
func runConcurrent[T any](items []T, run func(T) error) error {
	errs := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(item); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}
