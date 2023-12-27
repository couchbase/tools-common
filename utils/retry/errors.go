package retry

import (
	"errors"
	"fmt"
)

// RetriesExhaustedError is returned after exhausting the max number of retries, unwrapping the error will return the
// error from the last failure.
type RetriesExhaustedError struct {
	attempts int
	err      error
}

func (r *RetriesExhaustedError) Error() string {
	msg := fmt.Sprintf("exhausted retry count after %d attempts", r.attempts)
	if r.err != nil {
		msg += fmt.Sprintf(": %s", r.err)
	}

	return msg
}

func (r *RetriesExhaustedError) Unwrap() error {
	return r.err
}

// IsRetriesExhausted returns a boolean indicating whether the given error is a 'RetriesExhaustedError'.
func IsRetriesExhausted(err error) bool {
	var retriesExhausted *RetriesExhaustedError
	return errors.As(err, &retriesExhausted)
}

// RetriesAbortedError is returned when retries have been aborted either due to context cancellation, or through use of
// the 'AbortRetriesError' sentinel error.
type RetriesAbortedError struct {
	attempts int
	err      error
}

func (r *RetriesAbortedError) Error() string {
	msg := fmt.Sprintf("retries aborted after %d attempt(s)", r.attempts)
	if r.err != nil {
		msg += fmt.Sprintf(": %s", r.err)
	}

	return msg
}

func (r *RetriesAbortedError) Unwrap() error {
	return r.err
}

// IsRetriesAborted returns a boolean indicating whether the given error is a 'RetriesAbortedError'.
func IsRetriesAborted(err error) bool {
	var retriesAborted *RetriesAbortedError
	return errors.As(err, &retriesAborted)
}

// NewAbortRetriesError returns a wrapped error type, which allows aborting retries.
func NewAbortRetriesError(err error) error {
	return &AbortRetriesError{err: err}
}

// AbortRetriesError allows aborting retries mid-way in the event of a fatal error.
//
// NOTE: This error is not returned by the APIs, it's only used as a sentinel error to abort retries; expect a
// 'RetriesAbortedError' error.
type AbortRetriesError struct {
	err error
}

func (a *AbortRetriesError) Error() string {
	return fmt.Sprintf("retries aborted due to error: %s", a.err)
}

func (a *AbortRetriesError) Unwrap() error {
	return a.err
}
