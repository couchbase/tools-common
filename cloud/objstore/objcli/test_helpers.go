package objcli

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v2/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v2/objstore/objval"
	testutil "github.com/couchbase/tools-common/testing/util"
)

// TestUploadRAW uploads the given raw data.
func TestUploadRAW(t *testing.T, client Client, key string, body []byte) {
	err := client.PutObject(context.Background(), PutObjectOptions{
		Bucket: "bucket",
		Key:    key,
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)
}

// TestUploadJSON uploads the given data as JSON.
func TestUploadJSON(t *testing.T, client Client, key string, body any) {
	err := client.PutObject(context.Background(), PutObjectOptions{
		Bucket: "bucket",
		Key:    key,
		Body:   bytes.NewReader(testutil.MarshalJSON(t, body)),
	})
	require.NoError(t, err)
}

// TestDownloadRAW downloads the object as raw data.
func TestDownloadRAW(t *testing.T, client Client, key string) []byte {
	object, err := client.GetObject(context.Background(), GetObjectOptions{
		Bucket: "bucket",
		Key:    key,
	})
	require.NoError(t, err)

	defer object.Body.Close()

	return testutil.ReadAll(t, object.Body)
}

// TestDownloadJSON downloads the given object, unmarshaling it into the provided interface.
func TestDownloadJSON(t *testing.T, client Client, key string, data any) {
	object, err := client.GetObject(context.Background(), GetObjectOptions{
		Bucket: "bucket",
		Key:    key,
	})
	require.NoError(t, err)

	defer object.Body.Close()

	testutil.DecodeJSON(t, object.Body, &data)
}

// TestRequireKeyExists asserts that the given key exists.
func TestRequireKeyExists(t *testing.T, client Client, key string) {
	_, err := client.GetObjectAttrs(context.Background(), GetObjectAttrsOptions{
		Bucket: "bucket",
		Key:    key,
	})
	require.NoError(t, err)
}

// TestRequireKeyNotFound asserts that the given key does not exist.
func TestRequireKeyNotFound(t *testing.T, client Client, key string) {
	_, err := client.GetObjectAttrs(context.Background(), GetObjectAttrsOptions{
		Bucket: "bucket",
		Key:    key,
	})
	require.True(t, objerr.IsNotFoundError(err))
}

// TestListObjects returns the attributes of all the existing objects.
func TestListObjects(t *testing.T, client Client, prefix string) []*objval.ObjectAttrs {
	var (
		all = make([]*objval.ObjectAttrs, 0)
		fn  = func(attrs *objval.ObjectAttrs) error { all = append(all, attrs); return nil }
	)

	err := client.IterateObjects(context.Background(), IterateObjectsOptions{
		Bucket: "bucket",
		Prefix: prefix,
		Func:   fn,
	})
	require.NoError(t, err)

	return all
}
