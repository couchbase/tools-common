package objutil

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v3/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v3/objstore/objval"
)

func TestPrefixExistsOptionsDefaults(t *testing.T) {
	opts := PrefixExistsOptions{}
	opts.defaults()

	require.NotNil(t, opts.Context)
	require.Nil(t, opts.Client)
	require.Empty(t, opts.Bucket)
	require.Empty(t, opts.Prefix)
}

func TestPrefixExists(t *testing.T) {
	var (
		ctx    = context.Background()
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		bucket = "bucket"
		prefix = "prefix"
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: bucket,
		Key:    path.Join(prefix, "new_beginnings.txt"),
		Body:   strings.NewReader("Hello, World!"),
	})
	require.NoError(t, err)

	opts := PrefixExistsOptions{
		Context: ctx,
		Client:  client,
		Bucket:  bucket,
		Prefix:  prefix,
	}

	exists, err := PrefixExists(opts)
	require.NoError(t, err)
	require.True(t, exists)

	opts = PrefixExistsOptions{
		Context: ctx,
		Client:  client,
		Bucket:  bucket,
		Prefix:  "not-the-prefix",
	}

	exists, err = PrefixExists(opts)
	require.NoError(t, err)
	require.False(t, exists)
}
