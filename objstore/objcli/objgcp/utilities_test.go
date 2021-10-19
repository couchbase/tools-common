package objgcp

import (
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleError(t *testing.T) {
	var notFound *objerr.NotFoundError

	require.Nil(t, handleError("", "", nil))
	require.ErrorIs(t, handleError("", "", assert.AnError), assert.AnError)

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
}

func TestGenerateKey(t *testing.T) {
	require.True(t, strings.HasPrefix(generateKey("key"), "key-"))
	require.NotEqual(t, generateKey("key"), generateKey("key"))
}
