package objaws

import (
	"net"
	"testing"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v6/objstore/objerr"
	"github.com/couchbase/tools-common/types/v2/ptr"
)

func TestHandleError(t *testing.T) {
	var (
		notFound       *objerr.NotFoundError
		archiveStorage *objerr.ErrArchiveStorage
	)

	err := handleError(ptr.To("bucket1"), ptr.To("key1"), nil)
	require.NoError(t, err)

	// Not handled specifically but should not be <nil>
	err = handleError(ptr.To("bucket1"), ptr.To("key1"), &s3types.NoSuchUpload{})
	require.Error(t, err)

	err = handleError(ptr.To("bucket1"), ptr.To("key1"), &smithy.GenericAPIError{Code: "InvalidAccessKeyId"})
	require.ErrorIs(t, err, objerr.ErrUnauthenticated)

	err = handleError(ptr.To("bucket1"), ptr.To("key1"), &smithy.GenericAPIError{Code: "SignatureDoesNotMatch"})
	require.ErrorIs(t, err, objerr.ErrUnauthenticated)

	err = handleError(ptr.To("bucket1"), ptr.To("key1"), &smithy.GenericAPIError{Code: "AccessDenied"})
	require.ErrorIs(t, err, objerr.ErrUnauthorized)

	err = handleError(ptr.To("bucket1"), ptr.To("key1"), &s3types.NoSuchKey{})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "key", notFound.Type)
	require.Equal(t, "key1", notFound.Name)

	err = handleError(ptr.To("bucket1"), nil, &s3types.NoSuchKey{})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "key", notFound.Type)
	require.Equal(t, "<empty key name>", notFound.Name)

	err = handleError(ptr.To("bucket1"), ptr.To("key1"), &s3types.NoSuchBucket{})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "bucket", notFound.Type)
	require.Equal(t, "bucket1", notFound.Name)

	err = handleError(nil, ptr.To("key1"), &s3types.NoSuchBucket{})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "bucket", notFound.Type)
	require.Equal(t, "<empty bucket name>", notFound.Name)

	err = handleError(nil, nil, &net.DNSError{IsNotFound: true})
	require.ErrorIs(t, err, objerr.ErrEndpointResolutionFailed)

	err = handleError(nil, ptr.To("key1"), &s3types.InvalidObjectState{})
	require.ErrorAs(t, err, &archiveStorage)
	require.Equal(t, "key1", archiveStorage.Key)
}

func TestIsKeyNotFound(t *testing.T) {
	require.False(t, isKeyNotFound(assert.AnError))
	require.True(t, isKeyNotFound(&s3types.NoSuchKey{}))
	require.True(t, isKeyNotFound(&smithy.GenericAPIError{Code: "NotFound"}))
}

func TestIsNoSuchUpload(t *testing.T) {
	require.False(t, isNoSuchUpload(assert.AnError))
	require.True(t, isNoSuchUpload(&s3types.NoSuchUpload{}))
	require.True(t, isNoSuchUpload(&smithy.GenericAPIError{Code: "NotFound"}))
}
