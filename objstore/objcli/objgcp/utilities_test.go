package objgcp

import (
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/tools-common/objstore/objerr"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestHandleError(t *testing.T) {
	var notFound *objerr.NotFoundError

	require.Nil(t, handleError("", "", nil))
	require.ErrorIs(t, handleError("", "", assert.AnError), assert.AnError)

	require.ErrorIs(t,
		handleError("bucket", "key", &googleapi.Error{Code: http.StatusUnauthorized}), objerr.ErrUnauthenticated)

	require.ErrorIs(t,
		handleError("bucket", "key", &googleapi.Error{Code: http.StatusForbidden}), objerr.ErrUnauthorized)

	require.ErrorAs(t, handleError("", "", storage.ErrBucketNotExist), &notFound)
	require.Equal(t, "bucket", notFound.Type)
	require.Equal(t, "<empty bucket name>", notFound.Name)

	require.ErrorAs(t, handleError("bucket1", "", storage.ErrBucketNotExist), &notFound)
	require.Equal(t, "bucket", notFound.Type)
	require.Equal(t, "bucket1", notFound.Name)

	require.ErrorAs(t, handleError("", "", storage.ErrObjectNotExist), &notFound)
	require.Equal(t, "key", notFound.Type)
	require.Equal(t, "<empty key name>", notFound.Name)

	require.ErrorAs(t, handleError("", "key1", storage.ErrObjectNotExist), &notFound)
	require.Equal(t, "key", notFound.Type)
	require.Equal(t, "key1", notFound.Name)

	require.ErrorIs(t, handleError("", "", &net.DNSError{IsNotFound: true}), objerr.ErrEndpointResolutionFailed)
}

func TestPartKey(t *testing.T) {
	require.True(t, strings.HasPrefix(partKey("id", "key"), "key-"))
	require.NotEqual(t, partKey("id", "key"), partKey("id", "key"))
}

func TestPartPrefix(t *testing.T) {
	require.Equal(t, "/path/to/key-mpu-id", partPrefix("id", "/path/to/key"))
}
