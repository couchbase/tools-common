package objutil

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v9/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v9/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v9/objstore/objval"
	testutil "github.com/couchbase/tools-common/testing/util"
)

func TestCopyObject(t *testing.T) {
	type test struct {
		name     string
		provider objval.Provider
	}

	tests := []test{
		{
			name:     "SingleRequest",
			provider: objval.ProviderAWS,
		},
		{
			name:     "MultipleRequests",
			provider: 42,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				client = objcli.NewTestClient(t, test.provider)
				body   = []byte("Hello, World!")
			)

			attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
				Bucket:   "srcBucket",
				Key:      "srcKey",
				Body:     bytes.NewReader(body),
				Metadata: map[string]string{"key": "value"},
			})
			require.NoError(t, err)

			require.Equal(t, "srcKey", attrs.Key)
			require.Equal(t, int64(len(body)), *attrs.Size)
			require.NotEmpty(t, attrs.ETag)
			require.True(t, time.Now().After(*attrs.LastModified))

			options := CopyObjectOptions{
				Client:            client,
				DestinationBucket: "dstBucket",
				DestinationKey:    "dstKey",
				SourceBucket:      "srcBucket",
				SourceKey:         "srcKey",
			}

			attrs, err = CopyObject(options)
			require.NoError(t, err)

			require.Equal(t, "dstKey", attrs.Key)
			require.Equal(t, int64(len(body)), *attrs.Size)
			require.NotEmpty(t, attrs.ETag)
			require.True(t, time.Now().After(*attrs.LastModified))

			dst, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
				Bucket: "dstBucket",
				Key:    "dstKey",
			})
			require.NoError(t, err)
			require.Equal(t, body, testutil.ReadAll(t, dst.Body))
			require.Equal(t, map[string]string{"key": "value"}, dst.Metadata)
		})
	}
}

func TestCopyObjectPrecondition(t *testing.T) {
	type test struct {
		name     string
		provider objval.Provider
	}

	tests := []test{
		{
			name:     "SingleRequest",
			provider: objval.ProviderAWS,
		},
		{
			name:     "MultipleRequests",
			provider: 42,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := objcli.NewTestClient(t, test.provider)

			put := func(body string) string {
				_, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
					Bucket: "bucket",
					Key:    "srcKey",
					Body:   bytes.NewReader([]byte(body)),
				})
				require.NoError(t, err)

				attrs, err := client.GetObjectAttrs(context.Background(), objcli.GetObjectAttrsOptions{
					Bucket: "bucket",
					Key:    "srcKey",
				})
				require.NoError(t, err)
				require.NotEmpty(t, attrs.CAS)

				return attrs.CAS
			}

			options := CopyObjectOptions{
				Client:            client,
				DestinationBucket: "bucket",
				DestinationKey:    "dstKey",
				SourceBucket:      "bucket",
				SourceKey:         "srcKey",
				PreconditionData:  put("old"),
			}

			// The source was replaced after it was inspected, so the copy must be refused
			current := put("new")
			require.NotEqual(t, options.PreconditionData, current)

			_, err := CopyObject(options)

			var preconditionFailed *objerr.PreconditionFailedError

			require.ErrorAs(t, err, &preconditionFailed)

			options.PreconditionData = current

			_, err = CopyObject(options)
			require.NoError(t, err)

			dst, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
				Bucket: "bucket",
				Key:    "dstKey",
			})
			require.NoError(t, err)
			require.Equal(t, []byte("new"), testutil.ReadAll(t, dst.Body))
		})
	}
}

func TestMaxSingleOperationCopySize(t *testing.T) {
	type test struct {
		name     string
		provider objval.Provider
		expected int64
	}

	tests := []test{
		{
			name:     "AWS",
			provider: objval.ProviderAWS,
			expected: 5 * 1000 * 1000 * 1000,
		},
		{
			name:     "Azure",
			provider: objval.ProviderAzure,
			expected: 256 * 1000 * 1000,
		},
		{
			name:     "GCP",
			provider: objval.ProviderGCP,
			expected: math.MaxInt64,
		},
		{
			name:     "Other",
			provider: 42,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, maxSingleOperationCopySize(test.provider))
		})
	}
}
