package objazure

import (
	"errors"
	"net/http"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/couchbase/tools-common/objstore/objerr"
)

// handleError converts an error relating accessing an object via its key into a user friendly error where possible.
func handleError(bucket, key string, err error) error {
	var azureErr azblob.StorageError
	if err == nil || !errors.As(err, &azureErr) {
		return objerr.HandleError(err)
	}

	var (
		statusCode = -1
		resp       = azureErr.Response() //nolint:bodyclose
	)

	if resp != nil {
		statusCode = resp.StatusCode
	}

	switch statusCode {
	case http.StatusUnauthorized:
		return objerr.ErrUnauthenticated
	case http.StatusForbidden:
		return objerr.ErrUnauthorized
	}

	switch azureErr.ServiceCode() {
	case azblob.ServiceCodeBlobNotFound:
		// This shouldn't trigger but may aid in debugging in the future
		if key == "" {
			key = "<empty blob name>"
		}

		return &objerr.NotFoundError{Type: "blob", Name: key}
	case azblob.ServiceCodeContainerNotFound:
		// This shouldn't trigger but may aid in debugging in the future
		if bucket == "" {
			bucket = "<empty container name>"
		}

		return &objerr.NotFoundError{Type: "container", Name: bucket}
	}

	// This isn't a status code we plan to handle manually, return the complete error
	return err
}

// isKeyNotFound returns a boolean indicating whether the given error is a 'ServiceCodeBlobNotFound' error.
func isKeyNotFound(err error) bool {
	var azureErr azblob.StorageError
	return errors.As(err, &azureErr) && azureErr.ServiceCode() == azblob.ServiceCodeBlobNotFound
}
