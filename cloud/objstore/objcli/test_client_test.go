package objcli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
)

func TestIterateObjectsWithDelimiter(t *testing.T) {
	type test struct {
		name         string
		keysToCreate []string
		prefix       string
		expected     []string
	}

	tests := []test{
		{
			name:         "Prefix without /",
			keysToCreate: []string{"1/2/3/4", "1/asd.txt"},
			prefix:       "1",
			expected:     []string{"1/"},
		},
		{
			name:         "Prefix with /",
			keysToCreate: []string{"1/2/3/4", "1/asd.txt"},
			prefix:       "1/",
			expected:     []string{"1/2/", "1/asd.txt"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testClient := NewTestClient(t, objval.ProviderAWS)

			for _, key := range test.keysToCreate {
				attrs, err := testClient.PutObject(context.Background(), PutObjectOptions{
					Bucket: "bucket",
					Key:    key,
					Body:   strings.NewReader("asd"),
				})
				require.NoError(t, err)

				require.Equal(t, key, attrs.Key)
				require.Equal(t, int64(3), *attrs.Size)
				require.NotEmpty(t, attrs.ETag)
				require.True(t, time.Now().After(*attrs.LastModified))
			}

			secondLevelContent := make([]string, 0)

			err := testClient.IterateObjects(context.Background(), IterateObjectsOptions{
				Bucket:    "bucket",
				Prefix:    test.prefix,
				Delimiter: "/",
				Func: func(attrs *objval.ObjectAttrs) error {
					secondLevelContent = append(secondLevelContent, attrs.Key)

					return nil
				},
			})
			require.NoError(t, err)

			require.ElementsMatch(t, test.expected, secondLevelContent)
		})
	}
}

func TestIfMatch(t *testing.T) {
	cli := NewTestClient(t, objval.ProviderAWS)

	attrs, err := cli.PutObject(context.Background(), PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("foo"),
	})
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)
	require.Equal(t, int64(3), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	attrs, err = cli.GetObjectAttrs(context.Background(), GetObjectAttrsOptions{
		Bucket: "bucket",
		Key:    "key",
	})
	require.NoError(t, err)

	cas := attrs.CAS

	attrs, err = cli.PutObject(context.Background(), PutObjectOptions{
		Bucket:           "bucket",
		Key:              "key",
		Body:             strings.NewReader("bar"),
		Precondition:     OperationPreconditionIfMatch,
		PreconditionData: cas,
	})
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)
	require.Equal(t, int64(3), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	var precondErr *objerr.PreconditionFailedError

	_, err = cli.PutObject(context.Background(), PutObjectOptions{
		Bucket:           "bucket",
		Key:              "key",
		Body:             strings.NewReader("baz"),
		Precondition:     OperationPreconditionIfMatch,
		PreconditionData: cas,
	})

	require.ErrorAs(t, err, &precondErr)
}
