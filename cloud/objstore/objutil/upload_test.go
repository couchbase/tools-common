package objutil

import (
	"bytes"
	"strings"
	"testing"

	"github.com/couchbase/tools-common/cloud/v3/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v3/objstore/objval"

	"github.com/stretchr/testify/require"
)

func TestUploadOptionsDefaults(t *testing.T) {
	options := UploadOptions{}
	options.defaults()
	require.Equal(t, int64(MinPartSize), options.PartSize)
	require.Equal(t, int64(MPUThreshold), options.MPUThreshold)
}

func TestUploadObjectLessThanThreshold(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("body"),
	}

	require.NoError(t, Upload(options))
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)
	require.Contains(t, client.Buckets["bucket"], "key")
	require.Equal(t, []byte("body"), client.Buckets["bucket"]["key"].Body)
}

func TestUploadObjectGreaterThanThreshold(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   bytes.NewReader(make([]byte, MPUThreshold+1)),
	}

	require.NoError(t, Upload(options))
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)
	require.Contains(t, client.Buckets["bucket"], "key")
	require.Equal(t, make([]byte, MPUThreshold+1), client.Buckets["bucket"]["key"].Body)
}
