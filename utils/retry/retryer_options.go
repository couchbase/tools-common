package retry

import "time"

// Algorithm represents a retry algorithm used to determine backoff before retrying function execution.
type Algorithm int

const (
	// AlgorithmFibonacci backs off using the fibonacci sequence e.g. 50ms, 50ms, 100ms ... 128h9m33s
	AlgorithmFibonacci Algorithm = iota

	// AlgorithmExponential backs off exponentially e.g. 100ms, 200ms, 400ms ... 477218h35m18s
	AlgorithmExponential

	// AlgorithmLinear backs off linearly e.g. 50ms, 100ms, 150ms ... 1.75s
	AlgorithmLinear
)

// LogFunc is a function which is run before each retry attempt after failing to run the given 'RetryableFunc'.
type LogFunc[T any] func(ctx *Context, payload T, err error)

// ShouldRetryFunc is a function which may be supplied to the retry options which allows more control over which types
// of errors are retried.
//
// NOTE: If not supplied, retries will take place if the given 'RetryableFunc' returns an error.
type ShouldRetryFunc[T any] func(ctx *Context, payload T, err error) bool

// CleanupFunc is a function which is run with the payload for all, but the last retry attempt.
//
// NOTE: The final attempt is not cleaned up because the payload may want to be used/read to enhance returned errors.
type CleanupFunc[T any] func(payload T)

// RetryerOptions encapsulates the options available when creating a retryer.
type RetryerOptions[T any] struct {
	// Algorithm is the algorithm to use when calculating backoff.
	Algorithm Algorithm

	// MaxRetries is the maximum number of times to retry the function.
	MaxRetries int

	// MinDelay is the minimum delay to use for backoff.
	MinDelay time.Duration

	// MaxDelay is the maximum delay to use for backoff.
	MaxDelay time.Duration

	// MinJitter is the minimum amount of jitter to apply before backing off.
	MinJitter time.Duration

	// MaxJitter is the maximum amount of jitter to apply before backing off.
	MaxJitter time.Duration

	// ShouldRetry is a custom retry function, when not supplied, this will be defaulted to 'err != nil'.
	ShouldRetry ShouldRetryFunc[T]

	// Log is a function which is run before each retry, when not supplied logging will be skipped.
	Log LogFunc[T]

	// Cleanup is a cleanup function run for all but the last payloads prior to performing a retry.
	Cleanup CleanupFunc[T]
}

func (r *RetryerOptions[T]) defaults() {
	if r.MaxRetries == 0 {
		r.MaxRetries = 3
	}

	if r.MinDelay == 0 {
		r.MinDelay = 50 * time.Millisecond
	}

	if r.MaxDelay == 0 {
		r.MaxDelay = 2*time.Second + 500*time.Millisecond
	}

	if r.MinJitter == 0 {
		r.MinJitter = 50 * time.Millisecond
	}

	if r.MaxJitter == 0 {
		r.MaxJitter = 250 * time.Millisecond
	}
}
