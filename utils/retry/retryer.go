// Package retry exposes a 'Retryer' allowing conditionally retrying (with back-off) of functions.
package retry

import (
	"context"
	"errors"
	"math"
	"time"
)

// RetryableFunc represents a function which is retryable.
type RetryableFunc[T any] func(ctx *Context) (T, error)

// Retryer is a function retryer, which supports executing a given function a number of times until successful.
type Retryer[T any] struct {
	options RetryerOptions[T]
}

// NewRetryer returns a new retryer with the given options.
func NewRetryer[T any](options RetryerOptions[T]) Retryer[T] {
	// Not all options are required, but we use sane defaults otherwise behavior may be undesired/unexpected
	options.defaults()

	retryer := Retryer[T]{
		options: options,
	}

	return retryer
}

// Do executes the given function until it's successful.
func (r Retryer[T]) Do(fn RetryableFunc[T]) (T, error) {
	return r.DoWithContext(context.Background(), fn)
}

// DoWithContext executes the given function until it's successful, the provided context may be used for cancellation.
func (r Retryer[T]) DoWithContext(ctx context.Context, fn RetryableFunc[T]) (T, error) {
	var (
		wrapped = NewContext(ctx)
		payload T
		done    bool
		err     error
	)

	for ; wrapped.attempt <= r.options.MaxRetries; wrapped.attempt++ {
		payload, done, err = r.do(wrapped, fn)
		if done {
			return payload, err
		}

		// Log all but the last failure, the caller may use this to log that a retry is about to take place
		if r.options.Log != nil && wrapped.attempt != r.options.MaxRetries {
			r.options.Log(wrapped, payload, err)
		}
	}

	return payload, &RetriesExhaustedError{attempts: r.options.MaxRetries, err: err}
}

// do executes the given function, returning the payload error and whether retries should stop.
func (r Retryer[T]) do(ctx *Context, fn RetryableFunc[T]) (T, bool, error) {
	if err := ctx.Err(); err != nil {
		return *new(T), true, &RetriesAbortedError{attempts: ctx.attempt - 1, err: err}
	}

	payload, err := fn(ctx)

	// NOTE: The error returned by 'retry' may differ from the error defined above
	if retry, err := r.retry(ctx, payload, err); !retry {
		return payload, true, err
	}

	// NOTE: Run cleanup for all but the last attempt, the caller may want to use the payload from the final attempt
	if r.options.Cleanup != nil && ctx.attempt < r.options.MaxRetries {
		r.options.Cleanup(payload)
	}

	if err := r.sleep(ctx); err != nil {
		return *new(T), true, err
	}

	return payload, false, err
}

// retry returns a boolean indicating whether the function should be executed again.
//
// NOTE: Users may supply a custom 'ShouldRetry' function for more complex retry behavior which depends on the payload.
func (r Retryer[T]) retry(ctx *Context, payload T, err error) (bool, error) {
	var abort *AbortRetriesError

	// If the user has opted to abort retries, unwrap the error
	if errors.As(err, &abort) {
		return false, &RetriesAbortedError{attempts: ctx.attempt, err: abort.Unwrap()}
	}

	if r.options.ShouldRetry != nil {
		return r.options.ShouldRetry(ctx, payload, err), err
	}

	return err != nil, err
}

// sleep until the next retry attempt, or the given context is cancelled.
func (r Retryer[T]) sleep(ctx *Context) error {
	timer := time.NewTimer(r.Duration(ctx.Attempt()))
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return &RetriesAbortedError{attempts: ctx.attempt, err: ctx.Err()}
	}
}

// duration returns the duration to sleep for, this may be calculated using one of a number of different algorithms.
//
// NOTE: After fifty attempts, a constant duration is returned (the max available, or the chosen max delay).
func (r Retryer[T]) Duration(attempt int) time.Duration {
	// We truncate the attempt to fifty, to avoid overflowing the first multiplicand; this allows people to retry more
	// that fifty times but just hit a point where back-off is constant (or sits at their chosen max back-off).
	attempt = min(attempt, 50)

	var n time.Duration

	switch r.options.Algorithm {
	case AlgorithmLinear:
		n = time.Duration(attempt)
	case AlgorithmExponential:
		n = 1 << attempt
	case AlgorithmFibonacci:
		n = time.Duration(math.Round(math.Pow(math.Phi, float64(attempt)) / sqrt5))
	}

	duration := n * r.options.MinDelay

	// If we overflow, just return the max delay
	if n != duration/r.options.MinDelay {
		return r.options.MaxDelay
	}

	duration = max(r.options.MinDelay, duration)
	duration = min(r.options.MaxDelay, duration)

	return duration
}
