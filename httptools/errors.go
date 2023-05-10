package httptools

import (
	"errors"
	"fmt"

	"github.com/couchbase/tools-common/format"
)

// SocketClosedInFlightError is returned if the client socket was closed during an active request. This is usually due
// to socket being closed by the remote host in the event of a fatal error.
type SocketClosedInFlightError struct {
	method   string
	endpoint string
}

func (e *SocketClosedInFlightError) Error() string {
	return fmt.Sprintf("error executing '%s' request to '%s' socket closed in flight, check the logs for more details",
		e.method, e.endpoint)
}

// RetriesExhaustedError is returned if the REST request was retried the maximum number of times.
//
// NOTE: The number of times retried is configurable at runtime via an environment variable. See the 'Client'
// constructor for more information.
type RetriesExhaustedError struct {
	retries int
	err     error
}

func (e *RetriesExhaustedError) Error() string {
	return fmt.Sprintf("exhausted retry count after %d retries, last error: %s", e.retries, e.Unwrap())
}

func (e *RetriesExhaustedError) Unwrap() error {
	return e.err
}

// UnexpectedEndOfBodyError is returned if the length of the request body does not match the expected length. This may
// happen in the event that the 'Content-Length' header value is incorrectly set.
type UnexpectedEndOfBodyError struct {
	method   Method
	endpoint Endpoint
	expected int64
	got      int
}

func (e *UnexpectedEndOfBodyError) Error() string {
	return fmt.Sprintf("unexpected EOF whilst reading response body for '%s' request to '%s', expected %s but got %s",
		e.method, e.endpoint, format.Bytes(uint64(e.expected)), format.Bytes(uint64(e.got)))
}

// UnknownX509Error is returned when the dispatched REST request receives a generic (unhandled) x509 error.
type UnknownX509Error struct {
	inner error
}

func (e *UnknownX509Error) Unwrap() error {
	return e.inner
}

func (e *UnknownX509Error) Error() string {
	return e.inner.Error()
}

// UnexpectedStatusCodeError returned if a request was executed successfully, however, we received a response status
// code which was unexpected.
//
// NOTE: During development its possible to hit this error in the event that the expected status code is set incorrectly
// and the successful response does not return a body so is therefore something to watch out for.
type UnexpectedStatusCodeError struct {
	Status   int
	method   Method
	endpoint Endpoint
	Body     []byte
}

func (e *UnexpectedStatusCodeError) Error() string {
	msg := fmt.Sprintf("unexpected status code %d for '%s' request to '%s'", e.Status, e.method, e.endpoint)
	if len(e.Body) == 0 {
		msg += ", check the logs for more details"
	} else {
		msg += fmt.Sprintf(", %s", e.Body)
	}

	return msg
}

// AuthorizationError is returned if we receive a 403 status code which means the credentials are
// correct but they don't have the needed permissions.
type AuthorizationError struct {
	method      Method
	endpoint    Endpoint
	permissions []string
}

func (e *AuthorizationError) Error() string {
	if len(e.permissions) == 0 {
		return fmt.Sprintf("permission error executing '%s' request to '%s', user missing required permissions",
			e.method, e.endpoint)
	}

	return fmt.Sprintf("permission error executing '%s' request to '%s' required permissions are %v", e.method,
		e.endpoint, e.permissions)
}

// AuthenticationError is returned if we received a 401 status code i.e. the users credentials are incorrect.
type AuthenticationError struct {
	method   Method
	endpoint Endpoint
}

func (e *AuthenticationError) Error() string {
	return fmt.Sprintf("authentication error executing '%s' request to '%s' check credentials", e.method, e.endpoint)
}

// InternalServerError is returned if we received a 500 status code.
type InternalServerError struct {
	method   Method
	endpoint Endpoint
	Body     []byte
}

func (e *InternalServerError) Error() string {
	if e.Body != nil {
		return fmt.Sprintf("internal server error executing '%s' request to '%s': %s", e.method, e.endpoint, e.Body)
	}

	return fmt.Sprintf("internal server error executing '%s' request to '%s' check the logs for more details",
		e.method, e.endpoint)
}

// EndpointNotFoundError is returned if we received a 404 status code.
type EndpointNotFoundError struct {
	method   Method
	endpoint Endpoint
}

func (e *EndpointNotFoundError) Error() string {
	return fmt.Sprintf("received an unexpected 404 status executing '%s' request to '%s' check the logs for "+
		"more details", e.method, e.endpoint)
}

// IsEndpointNotFound returns a boolean indicating whether the given error is an 'EndpointNotFoundError'.
func IsEndpointNotFound(err error) bool {
	var notFound *EndpointNotFoundError
	return err != nil && errors.As(err, &notFound)
}
