package objerr

import "errors"

// ErrUnsupportedOperation is returned when attempting to perform an operation which is unsupported for the current
// cloud provider.
var ErrUnsupportedOperation = errors.New("unsupported operation")
