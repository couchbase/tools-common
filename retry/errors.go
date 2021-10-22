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

// RetriesAbortedError is returned when retries have been aborted for some reason, likely due to a context cancellation.
type RetriesAbortedError struct {
	attempts int
	err      error
}

func (r *RetriesAbortedError) Error() string {
	msg := fmt.Sprintf("retries aborted after %d attempts", r.attempts)
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
