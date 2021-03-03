package retry

import (
	"errors"
	"fmt"
)

// ErrIsCancelled is returned if the provided context was cancelled prior to complete/timeout.
var ErrIsCancelled = errors.New("operation was cancelled")

// RetriesExhaustedError is returned after exhausting the max number of retries, unwrapping the error will return the
// error from the last failure.
type RetriesExhaustedError struct {
	retries int
	err     error
}

func (e RetriesExhaustedError) Error() string {
	return fmt.Sprintf("exhausted retry count after %d retries: %v", e.retries, e.err)
}

func (e RetriesExhaustedError) Unwrap() error {
	return e.err
}
