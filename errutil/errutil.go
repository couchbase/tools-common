package errutil

import (
	"errors"
	"strings"
)

// Contains returns a boolean indicating whether the given error contained the given substring.
//
// NOTE: A <nil> error will always return false.
func Contains(err error, substr string) bool {
	return err != nil && strings.Contains(err.Error(), substr)
}

// Unwrap completely unwraps an error, returning the source/root error.
func Unwrap(err error) error {
	for err != nil {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			break
		}

		err = unwrapped
	}

	return err
}
