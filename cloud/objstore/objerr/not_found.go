package objerr

import (
	"errors"
	"fmt"
)

// NotFoundError indicates that something was not found.
type NotFoundError struct {
	Type string
	Name string
}

// Error implements the 'error' interface.
func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s '%s' not found", e.Type, e.Name)
}

// IsNotFoundError return a boolean indicating whether the given error is a 'NotFoundError'.
func IsNotFoundError(err error) bool {
	var notFoundError *NotFoundError
	return errors.As(err, &notFoundError)
}
