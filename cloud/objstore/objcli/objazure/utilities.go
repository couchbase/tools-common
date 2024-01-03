package objazure

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/couchbase/tools-common/cloud/v3/objstore/objerr"
)

// handleError converts an error relating accessing an object via its key into a user friendly error where possible.
func handleError(bucket, key string, err error) error {
	if bloberror.HasCode(err, bloberror.AuthenticationFailed) {
		return objerr.ErrUnauthenticated
	}

	if bloberror.HasCode(err, bloberror.AuthorizationFailure) {
		return objerr.ErrUnauthorized
	}

	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		// This shouldn't trigger but may aid in debugging in the future
		if key == "" {
			key = "<empty blob name>"
		}

		return &objerr.NotFoundError{Type: "blob", Name: key}
	}

	if bloberror.HasCode(err, bloberror.ContainerNotFound) {
		// This shouldn't trigger but may aid in debugging in the future
		if bucket == "" {
			bucket = "<empty container name>"
		}

		return &objerr.NotFoundError{Type: "container", Name: bucket}
	}

	return objerr.HandleError(err)
}

// isKeyNotFound returns a boolean indicating whether the given error is a 'ServiceCodeBlobNotFound' error.
func isKeyNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}
