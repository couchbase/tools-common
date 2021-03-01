package retry

import (
	"context"
	"time"
)

type LogFn func(attempt int)

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
