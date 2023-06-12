package cbrest

import (
	"errors"
	"fmt"
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

	// ErrStreamWithTimeout is returned if the user attempts to execute a stream with a non-zero timeout.
	ErrStreamWithTimeout = errors.New("using a timeout when executing a streaming request is unsupported")

	// ErrInvalidNetwork is returned if the user supplies an invalid value for the 'network' query parameter.
	ErrInvalidNetwork = errors.New("invalid use of 'network' query parameter, expected 'default' or 'external'")
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

// OldClusterConfigError is returned when the client attempts to bootstrap against a node which returns a cluster config
// which is older than the one we already have.
type OldClusterConfigError struct {
	old, curr int64
}

func (e *OldClusterConfigError) Error() string {
	return fmt.Sprintf("cluster config revision %d is older than the current revision %d", e.old, e.curr)
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
