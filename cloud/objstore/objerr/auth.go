// Package objerr provides error definitions used in 'objstore'.
package objerr

import "errors"

var (
	// ErrUnauthenticated is returned if we've sent a request to a cloud provider and received a response indicating
	// that we're unauthenticated i.e. 401 for Azure and typically a 403 for AWS.
	ErrUnauthenticated = errors.New("failed to authenticate, please check that valid credentials have been provided")

	// ErrUnauthorized is returned if we've successfully authenticated against the cloud provider, however, we've
	// attempted an operation where we don't have the valid permissions. This is typically a result of not having the
	// correct RBAC permissions.
	//
	// NOTE: This error message is inaccurate for S3. Neither "403 AccessDenied" nor "403 Forbidden" guarantee that the
	// user has been authenticated successfully.
	ErrUnauthorized = errors.New("authenticated user does not have the permission to access this resource")
)
