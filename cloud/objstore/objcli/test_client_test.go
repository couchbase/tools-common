package objcli

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
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
				err := testClient.PutObject(context.Background(), PutObjectOptions{
					Bucket: "bucket",
					Key:    key,
					Body:   strings.NewReader("asd"),
				})
				require.NoError(t, err)
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
	require.NoError(t, cli.PutObject(context.Background(), PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("foo"),
	}))

	attrs, err := cli.GetObjectAttrs(context.Background(), GetObjectAttrsOptions{
		Bucket: "bucket",
		Key:    "key",
	})
	require.NoError(t, err)

	cas := attrs.CAS

	require.NoError(t, cli.PutObject(context.Background(), PutObjectOptions{
		Bucket:           "bucket",
		Key:              "key",
		Body:             strings.NewReader("bar"),
		Precondition:     OperationPreconditionIfMatch,
		PreconditionData: cas,
	}))

	var precondErr *objerr.PreconditionFailedError

	require.ErrorAs(
		t,
		cli.PutObject(context.Background(), PutObjectOptions{
			Bucket:           "bucket",
			Key:              "key",
			Body:             strings.NewReader("baz"),
			Precondition:     OperationPreconditionIfMatch,
			PreconditionData: cas,
		}),
		&precondErr)
}
