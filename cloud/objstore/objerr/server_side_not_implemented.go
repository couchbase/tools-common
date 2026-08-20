package objerr

import "errors"

// ErrServerSideNotImplemented is returned when the object store rejects a request because it doesn't implement
// the requested operation.
var ErrServerSideNotImplemented = errors.New("operation is not implemented by the object store")
