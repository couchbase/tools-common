package retry

import (
	"errors"
	"fmt"
)

var ErrIsCancelled = errors.New("operation was cancelled")

type RetriesExhaustedError struct {
	retries int
	err     error
}

func (e RetriesExhaustedError) Error() string {
	return fmt.Sprintf("exhausted retry count after %d retries. %v", e.retries, e.err)
}

func (e RetriesExhaustedError) Unwrap() error {
	return e.err
}
