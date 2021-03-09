package cbrest

import (
	"errors"
	"fmt"

	"github.com/couchbase/tools-common/format"
)

var (
	// errExhaustedBootstrapHosts is an internal error, which signals that we've run out of valid hosts to bootstrap
	// from.
	errExhaustedBootstrapHosts = errors.New("exhausted bootstrap hosts")

	// ErrNodeUninitialized is returned if the user attempts to interact with a node which has not been initialized.
	ErrNodeUninitialized = errors.New("attempted to connect to an uninitialized node")
)

// BootstrapFailureError is returned to the user if we've failed to bootstrap the REST client.
//
// NOTE: The error message varies depending on whether we received at least one 401 when attempting to bootstrap.
type BootstrapFailureError struct {
	unauthorized bool
}

func (e *BootstrapFailureError) Error() string {
	msg := "failed to connect to any host(s) from the connection string"
	if e.unauthorized {
		msg += ", check username and password"
	} else {
		msg += ", check the logs for more details"
	}

	return msg
}

// UnauthorizedError is returned if we received a 401 status code from the cluster i.e. the users credentials are
// incorrect.
type UnauthorizedError struct {
	method   Method
	endpoint Endpoint
}

func (e *UnauthorizedError) Error() string {
	return fmt.Sprintf("authentication error executing '%s' request to '%s' check credentials", e.method, e.endpoint)
}

// InternalServerError is returned if we received a 500 status code from the cluster.
type InternalServerError struct {
	method   Method
	endpoint Endpoint
}

func (e *InternalServerError) Error() string {
	return fmt.Sprintf("internal server error executing '%s' request to '%s' check the logs for more details",
		e.method, e.endpoint)
}

// EndpointNotFoundError is returned if we received a 404 status code from the cluster.
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

// UnexpectedStatusCodeError returned if a request was executed successfully, however, we received a response status
// code which was unexpected.
//
// NOTE: During development its possible to hit this error in the event that the expected status code is set incorrectly
// and the successful response does not return a body so is therefore something to watch out for.
type UnexpectedStatusCodeError struct {
	status   int
	method   Method
	endpoint Endpoint
	empty    bool
}

func (e *UnexpectedStatusCodeError) Error() string {
	msg := fmt.Sprintf("unexpected status code %d for '%s' request to '%s'", e.status, e.method, e.endpoint)
	if e.empty {
		msg += " response body was empty"
	}

	return msg + ", check the logs for more details"
}

// ServiceNotAvailableError is returned if the requested service is is unavailable i.e. there are no nodes in the
// cluster running that service.
type ServiceNotAvailableError struct {
	service Service
}

func (e *ServiceNotAvailableError) Error() string {
	return fmt.Sprintf("%s Service is not available", e.service)
}

// IsServiceNotAvailable returns a boolean indicating whether the given error is a 'ServiceNotAvailableError'.
func IsServiceNotAvailable(err error) bool {
	var notAvailable *ServiceNotAvailableError
	return err != nil && errors.As(err, &notAvailable)
}

// UnknownAuthorityError returned when the dispatched REST request receives an 'UnknownAuthorityError'.
type UnknownAuthorityError struct {
	inner error
}

func (e *UnknownAuthorityError) Error() string {
	return fmt.Sprintf("%s\n\nIf you are using self-signed certificates you can re-run this command with\nthe "+
		"--no-ssl-verify flag. Note however that disabling ssl verification\nmeans that cbbackupmgr will be "+
		"vulnerable to man-in-the-middle attacks.\n\nFor the most secure access to Couchbase make sure that you "+
		"have X.509\ncertificates set up in your cluster and use the --cacert flag to specify\nyour client "+
		"certificate.", e.inner)
}

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

// RetriesExhaustedError is returned if the REST request was retried the maximum number of times.
//
// NOTE: The number of times retried is configurable at runtime via an environment variable. See the 'Client'
// constructor for more information.
type RetriesExhaustedError struct {
	retries int
	codes   []int
}

func (e RetriesExhaustedError) Error() string {
	return fmt.Sprintf("exhausted retry count after %d retries with status codes %v", e.retries, e.codes)
}