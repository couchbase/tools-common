package retry

import (
	"time"

	"github.com/couchbase/tools-common/maths"
)

// Algoritmn represents a retry algoritmn used to determine backoff before retrying function execution.
type Algoritmn int

const (
	// AlgoritmnFibonacci backs off using the fibonacci sequence e.g. 50ms, 50ms, 100ms ... 128h9m33s
	AlgoritmnFibonacci Algoritmn = iota

	// AlgoritmnExponential backs off exponentially e.g. 100ms, 200ms, 400ms ... 477218h35m18s
	AlgoritmnExponential

	// AlgoritmnLinear backs off linearly e.g. 50ms, 100ms, 150ms ... 1.75s
	AlgoritmnLinear
)

// LogFunc is a function which is run before each retry attempt after failing to run the given 'RetryableFunc'.
type LogFunc func(ctx *Context, payload interface{}, err error)

// ShouldRetryFunc is a function which may be supplied to the retry options which allows more control over which types
// of errors are retried.
//
// NOTE: If not supplied, retries will take place if the given 'RetryableFunc' returns an error.
type ShouldRetryFunc func(ctx *Context, payload interface{}, err error) bool

// CleanupFunc is a function which is run with the payload for all, but the last retry attempt.
//
// NOTE: The final attempt is not cleaned up because the payload may want to be used/read to enhance returned errors.
type CleanupFunc func(payload interface{})

// RetryerOptions encapsulates the options available when creating a retryer.
type RetryerOptions struct {
	// Algoritmn is the algorithm to use when calculating backoff.
	Algoritmn Algoritmn

	// MaxRetries is the maximum number of times to retry the function.
	MaxRetries int

	// MinDelay is the minimum delay to use for backoff.
	MinDelay time.Duration

	// MaxDelay is the maximum delay to use for backoff.
	MaxDelay time.Duration

	// ShouldRetry is a custom retry function, when not supplied, this will be defaulted to 'err != nil'.
	ShouldRetry ShouldRetryFunc

	// Log is a function which is run before each retry, when not supplied logging will be skipped.
	Log LogFunc

	// Cleanup is a cleanup function run for all but the last payloads prior to performing a retry.
	Cleanup CleanupFunc
}

func (r *RetryerOptions) defaults() {
	if r.MaxRetries == 0 {
		r.MaxRetries = 3
	}

	// NOTE: Limit the user to supplying 50 retries, this avoids the possibility for an overflow when generating the
	// first multiplicand.
	r.MaxRetries = maths.Min(r.MaxRetries, 50)

	if r.MinDelay == 0 {
		r.MinDelay = 50 * time.Millisecond
	}

	if r.MaxDelay == 0 {
		r.MaxDelay = 2*time.Second + 500*time.Millisecond
	}
}
