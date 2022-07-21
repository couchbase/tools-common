package objcli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/testutil"
)

// TestUploadRAW uploads the given raw data.
func TestUploadRAW(t *testing.T, client Client, key string, body []byte) {
	require.NoError(t, client.PutObject("bucket", key, bytes.NewReader(body)))
}

// TestUploadJSON uploads the given data as JSON.
func TestUploadJSON(t *testing.T, client Client, key string, body any) {
	require.NoError(t, client.PutObject("bucket", key, bytes.NewReader(testutil.MarshalJSON(t, body))))
}

// TestDownloadRAW downloads the object as raw data.
func TestDownloadRAW(t *testing.T, client Client, key string) []byte {
	object, err := client.GetObject("bucket", key, nil)
	require.NoError(t, err)

	defer object.Body.Close()

	return testutil.ReadAll(t, object.Body)
}

// TestDownloadJSON downloads the given object, unmarshaling it into the provided interface.
func TestDownloadJSON(t *testing.T, client Client, key string, data any) {
	object, err := client.GetObject("bucket", key, nil)
	require.NoError(t, err)

	defer object.Body.Close()

	testutil.DecodeJSON(t, object.Body, &data)
}

// TestRequireKeyExists asserts that the given key exists.
func TestRequireKeyExists(t *testing.T, client Client, key string) {
	_, err := client.GetObjectAttrs("bucket", key)
	require.NoError(t, err)
}

// TestRequireKeyNotFound asserts that the given key does not exist.
func TestRequireKeyNotFound(t *testing.T, client Client, key string) {
	_, err := client.GetObjectAttrs("bucket", key)
	require.True(t, objerr.IsNotFoundError(err))
}

// TestListObjects returns the attributes of all the existing objects.
func TestListObjects(t *testing.T, client Client, prefix string) []*objval.ObjectAttrs {
	var (
		all = make([]*objval.ObjectAttrs, 0)
		fn  = func(attrs *objval.ObjectAttrs) error { all = append(all, attrs); return nil }
	)

	require.NoError(t, client.IterateObjects("bucket", prefix, "", nil, nil, fn))

	return all
}
