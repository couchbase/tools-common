package objerr

import (
	"errors"
	"net"
)

// HandleError converts the given error into a user friendly error where possible, returning the given error when not.
func HandleError(err error) error {
	if err := TryHandleError(err); err != nil {
		return err
	}

	return err
}

// TryHandleError converts the given error into a user friendly error where possible, returning <nil> where not.
func TryHandleError(err error) error {
	var dnsError *net.DNSError

	if errors.As(err, &dnsError) && dnsError.IsNotFound {
		return ErrEndpointResolutionFailed
	}

	return nil
}
