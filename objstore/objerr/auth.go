package objerr

import "errors"

var (
	// ErrUnauthenticated is returned if we've sent a request to a cloud provider and received a response indicating
	// that we're unauthenticated i.e. 401 for Azure and typically a 403 for AWS.
	ErrUnauthenticated = errors.New("failed to authenticate, please check that valid credentials have been provided")

	// ErrUnauthorized is returned if we've successfully authenticated against the cloud provider, however, we've
	// attempted an operation where we don't have the valid permissions. This is typically a result of not having the
	// correct RBAC permissions.
	ErrUnauthorized = errors.New("authenticated user does not have the permission to access this resource")
)
