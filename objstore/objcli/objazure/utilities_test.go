package objazure

import (
	"net"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/objstore/objerr"
)

func TestHandleError(t *testing.T) {
	err := handleError("", "", &net.DNSError{IsNotFound: true})
	require.ErrorIs(t, err, objerr.ErrEndpointResolutionFailed)

	var notFound *objerr.NotFoundError

	err = handleError("container1", "blob1", nil)
	require.NoError(t, err)

	// Not handled specifically but should not be <nil>
	err = handleError("container1", "blob1",
		&azblob.StorageError{ErrorCode: azblob.StorageErrorCodeBlobBeingRehydrated})
	require.Error(t, err)

	err = handleError("container1", "blob1", storageError(http.StatusUnauthorized))
	require.ErrorIs(t, err, objerr.ErrUnauthenticated)

	err = handleError("container1", "blob1", storageError(http.StatusForbidden))
	require.ErrorIs(t, err, objerr.ErrUnauthorized)

	err = handleError("container1", "blob1", &azblob.StorageError{ErrorCode: azblob.StorageErrorCodeBlobNotFound})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "blob", notFound.Type)
	require.Equal(t, "blob1", notFound.Name)

	err = handleError("container1", "", &azblob.StorageError{ErrorCode: azblob.StorageErrorCodeBlobNotFound})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "blob", notFound.Type)
	require.Equal(t, "<empty blob name>", notFound.Name)

	err = handleError("container1", "blob1", &azblob.StorageError{ErrorCode: azblob.StorageErrorCodeContainerNotFound})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "container", notFound.Type)
	require.Equal(t, "container1", notFound.Name)

	err = handleError("", "blob1", &azblob.StorageError{ErrorCode: azblob.StorageErrorCodeContainerNotFound})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "container", notFound.Type)
	require.Equal(t, "<empty container name>", notFound.Name)
}

func TestIsKeyNotFound(t *testing.T) {
	require.False(t, isKeyNotFound(assert.AnError))
	require.True(t, isKeyNotFound(&azblob.StorageError{ErrorCode: azblob.StorageErrorCodeBlobNotFound}))
}
