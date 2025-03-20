package objazure

import (
	"net"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objerr"
)

func respError(code bloberror.Code) *azcore.ResponseError {
	return &azcore.ResponseError{ErrorCode: string(code)}
}

func TestHandleError(t *testing.T) {
	err := handleError("", "", &net.DNSError{IsNotFound: true})
	require.ErrorIs(t, err, objerr.ErrEndpointResolutionFailed)

	var (
		notFound       *objerr.NotFoundError
		archiveStorage *objerr.ErrArchiveStorage
	)

	err = handleError("container1", "blob1", nil)
	require.NoError(t, err)

	// Not handled specifically but should not be <nil>
	err = handleError("container1", "blob1", respError(bloberror.BlobBeingRehydrated))
	require.Error(t, err)

	err = handleError("container1", "blob1", respError(bloberror.AuthenticationFailed))
	require.ErrorIs(t, err, objerr.ErrUnauthenticated)

	err = handleError("container1", "blob1", respError(bloberror.AuthorizationFailure))
	require.ErrorIs(t, err, objerr.ErrUnauthorized)

	err = handleError("container1", "blob1", respError(bloberror.BlobNotFound))
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "blob", notFound.Type)
	require.Equal(t, "blob1", notFound.Name)

	err = handleError("container1", "", respError(bloberror.BlobNotFound))
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "blob", notFound.Type)
	require.Equal(t, "<empty blob name>", notFound.Name)

	err = handleError("container1", "blob1", respError(bloberror.ContainerNotFound))
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "container", notFound.Type)
	require.Equal(t, "container1", notFound.Name)

	err = handleError("", "blob1", respError(bloberror.ContainerNotFound))
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "container", notFound.Type)
	require.Equal(t, "<empty container name>", notFound.Name)

	err = handleError("container1", "blob1", respError(bloberror.BlobArchived))
	require.ErrorAs(t, err, &archiveStorage)
	require.Equal(t, "blob1", archiveStorage.Key)
}

func TestIsKeyNotFound(t *testing.T) {
	require.False(t, isKeyNotFound(assert.AnError))
	require.True(t, isKeyNotFound(respError(bloberror.BlobNotFound)))
}
