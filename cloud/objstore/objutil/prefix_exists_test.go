package objutil

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v6/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v6/objstore/objval"
	"github.com/couchbase/tools-common/testing/mock/matchers"
	"github.com/couchbase/tools-common/types/v2/ptr"
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

func TestPrefixExistsSentinel(t *testing.T) {
	cli := objcli.MockClient{}
	cli.
		On("IterateObjects", matchers.Context, mock.Anything).
		Return(func(_ context.Context, opts objcli.IterateObjectsOptions) error {
			err := opts.Func(&objval.ObjectAttrs{
				Key:  "foo",
				Size: ptr.To[int64](147),
			})
			if err != nil {
				return fmt.Errorf("failed processing page: %w", err)
			}

			return nil
		})

	exists, err := PrefixExists(PrefixExistsOptions{
		Client: &cli,
		Bucket: "bucket",
		Prefix: "prefix",
	})
	require.NoError(t, err)
	require.True(t, exists)
}
