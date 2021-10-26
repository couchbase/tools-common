package cbrest

import (
	"errors"
	"fmt"

	"github.com/couchbase/tools-common/format"
)

var (
	// ErrNodeUninitialized is returned if the user attempts to interact with a node which has not been initialized.
	ErrNodeUninitialized = errors.New("attempted to connect to an uninitialized node")

	// ErrNotBootstrapped is returned when attempting to retrieve hosts from an auth provider which has not been
	// bootstrapped; this error shouldn't be returned when using the 'Client' since the constructor performs
	// bootstrapping.
	ErrNotBootstrapped = errors.New("auth provider not bootstrapped")

	// ErrExhaustedClusterNodes is returned if we've failed to update the clients cluster config and have run out of
	// nodes.
	ErrExhaustedClusterNodes = errors.New("exhausted cluster nodes")

	// ErrThisNodeOnlyExpectsASingleAddress is returned if the user attempts to connect to a single node, but provides
	// more than one node in the connection string.
	ErrThisNodeOnlyExpectsASingleAddress = errors.New("when using 'ThisNodeOnly', a connection string with a single " +
		"address should be supplied")

	// ErrConnectionModeRequiresNonTLS is returned if the user attempts to connect using TLS when the connection mode
	// requires non-TLS communication.
	ErrConnectionModeRequiresNonTLS = errors.New("connection mode requires non-TLS communication")
)

// BootstrapFailureError is returned to the user if we've failed to bootstrap the REST client.
//
// NOTE: The error message varies depending on whether we received at least one 401 when attempting to bootstrap.
type BootstrapFailureError struct {
	ErrAuthentication error
	ErrAuthorization  error
}

func (e *BootstrapFailureError) Error() string {
	msg := "failed to connect to any host(s) from the connection string"
	if e.ErrAuthentication != nil {
		msg += ", check username and password"
	} else if e.ErrAuthorization != nil {
		msg += ", user does not have the required permissions"
	} else {
		msg += ", check the logs for more details"
	}

	return msg
}

// AuthorizationError is returned if we receive a 403 status code from the cluster which means the credentials are
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

// AuthenticationError is returned if we received a 401 status code from the cluster i.e. the users credentials are
// incorrect.
type AuthenticationError struct {
	method   Method
	endpoint Endpoint
}

func (e *AuthenticationError) Error() string {
	return fmt.Sprintf("authentication error executing '%s' request to '%s' check credentials", e.method, e.endpoint)
}

// InternalServerError is returned if we received a 500 status code from the cluster.
type InternalServerError struct {
	method   Method
	endpoint Endpoint
	body     []byte
}

func (e *InternalServerError) Error() string {
	if e.body != nil {
		return fmt.Sprintf("internal server error executing '%s' request to '%s': %s", e.method, e.endpoint, e.body)
	}

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
	Status   int
	method   Method
	endpoint Endpoint
	body     []byte
}

func (e *UnexpectedStatusCodeError) Error() string {
	msg := fmt.Sprintf("unexpected status code %d for '%s' request to '%s'", e.Status, e.method, e.endpoint)
	if len(e.body) == 0 {
		msg += ", check the logs for more details"
	} else {
		msg += fmt.Sprintf(", %s", e.body)
	}

	return msg
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

// UnknownAuthorityError is returned when the dispatched REST request receives an 'UnknownAuthorityError'.
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
	err     error
}

func (e *RetriesExhaustedError) Error() string {
	return fmt.Sprintf("exhausted retry count after %d retries, last error: %s", e.retries, e.Unwrap())
}

func (e *RetriesExhaustedError) Unwrap() error {
	return e.err
}

// OldClusterConfigError is returned when the client attempts to bootstrap against a node which returns a cluster config
// which is older than the one we already have.
type OldClusterConfigError struct {
	old, curr int64
}

func (e *OldClusterConfigError) Error() string {
	return fmt.Sprintf("cluster config revision %d is older than the current revision %d", e.old, e.curr)
}
