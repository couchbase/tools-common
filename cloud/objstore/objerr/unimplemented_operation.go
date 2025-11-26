package objerr

import (
	"errors"
	"fmt"
)

// UnimplementedOperationError indicates that something was not found.
type UnimplementedOperationError struct {
	Name string
}

// Error implements the 'error' interface.
func (u *UnimplementedOperationError) Error() string {
	return fmt.Sprintf("operation %q is not implemented", u.Name)
}

// UnimplementedOperationError return a boolean indicating whether the given error is a 'UnimplementedOperationError'.
func IsUnimplementedOperationError(err error) bool {
	var unimplementedOperationError *UnimplementedOperationError
	return errors.As(err, &unimplementedOperationError)
}
