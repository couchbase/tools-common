package retry

import (
	"context"
	"time"
)

// LogFn is is a readability wrapper around the function which can be passed to retry functions to allow logging prior
// to each retry.
type LogFn func(attempt int)

// ExponentialWithContext retries the provided function an constrained number of times, backing off exponentially
// between each failure.
func ExponentialWithContext(ctx context.Context, maxRetries int, wait time.Duration, fn func() error, log LogFn) error {
	var err error

	for tries := 0; tries < maxRetries; tries++ {
		if log != nil {
			log(tries)
		}

		err = fn()
		if err == nil {
			return nil
		}

		if cancelErr := cancellableSleep(ctx, time.Duration(tries+1)*wait); cancelErr != nil {
			return cancelErr
		}
	}

	if err != nil {
		return RetriesExhaustedError{retries: maxRetries, err: err}
	}

	return nil
}

func cancellableSleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ErrIsCancelled
	}
}
