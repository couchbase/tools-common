package objaws

import (
	"net"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleError(t *testing.T) {
	var notFound *objerr.NotFoundError

	err := handleError(aws.String("bucket1"), aws.String("key1"), nil)
	require.NoError(t, err)

	// Not handled specifically but should not be <nil>
	err = handleError(aws.String("bucket1"), aws.String("key1"), &mockError{inner: s3.ErrCodeNoSuchUpload})
	require.Error(t, err)

	err = handleError(aws.String("bucket1"), aws.String("key1"), &mockError{inner: "InvalidAccessKeyId"})
	require.ErrorIs(t, err, objerr.ErrUnauthenticated)

	err = handleError(aws.String("bucket1"), aws.String("key1"), &mockError{inner: "SignatureDoesNotMatch"})
	require.ErrorIs(t, err, objerr.ErrUnauthenticated)

	err = handleError(aws.String("bucket1"), aws.String("key1"), &mockError{inner: "AccessDenied"})
	require.ErrorIs(t, err, objerr.ErrUnauthorized)

	err = handleError(aws.String("bucket1"), aws.String("key1"), &mockError{inner: s3.ErrCodeNoSuchKey})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "key", notFound.Type)
	require.Equal(t, "key1", notFound.Name)

	err = handleError(aws.String("bucket1"), nil, &mockError{inner: s3.ErrCodeNoSuchKey})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "key", notFound.Type)
	require.Equal(t, "<empty key name>", notFound.Name)

	err = handleError(aws.String("bucket1"), aws.String("key1"), &mockError{inner: s3.ErrCodeNoSuchBucket})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "bucket", notFound.Type)
	require.Equal(t, "bucket1", notFound.Name)

	err = handleError(nil, aws.String("key1"), &mockError{inner: s3.ErrCodeNoSuchBucket})
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "bucket", notFound.Type)
	require.Equal(t, "<empty bucket name>", notFound.Name)

	err = handleError(nil, nil, &net.DNSError{IsNotFound: true})
	require.ErrorIs(t, err, objerr.ErrEndpointResolutionFailed)

	err = handleError(nil, nil, &mockError{inner: aws.ErrMissingEndpoint.Code()})
	require.ErrorIs(t, err, objerr.ErrEndpointResolutionFailed)
}

func TestIsKeyNotFound(t *testing.T) {
	require.False(t, isKeyNotFound(assert.AnError))
	require.True(t, isKeyNotFound(&mockError{inner: sns.ErrCodeNotFoundException}))
	require.True(t, isKeyNotFound(&mockError{inner: s3.ErrCodeNoSuchKey}))
}

func TestIsNoSuchUpload(t *testing.T) {
	require.False(t, isNoSuchUpload(assert.AnError))
	require.True(t, isNoSuchUpload(&mockError{inner: sns.ErrCodeNotFoundException}))
	require.True(t, isNoSuchUpload(&mockError{inner: s3.ErrCodeNoSuchUpload}))
}
