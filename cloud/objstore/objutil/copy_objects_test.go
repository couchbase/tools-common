package objutil

import (
	"bytes"
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
	testutil "github.com/couchbase/tools-common/testing/util"
)

func TestCopyObjectsSamePrefix(t *testing.T) {
	options := CopyObjectsOptions{
		DestinationPrefix: "prefix",
		SourcePrefix:      "prefix",
	}

	_, err := CopyObjects(options)
	require.ErrorIs(t, err, ErrCopyToSamePrefix)
}

func TestCopyObjects(t *testing.T) {
	var (
		client   = objcli.NewTestClient(t, objval.ProviderAWS)
		contents = [][]byte{[]byte("1"), []byte("2")}
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(contents[0]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key1", attrs.Key)
	require.Equal(t, int64(len(contents[0])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	attrs, err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key2",
		Body:   bytes.NewReader(contents[1]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key2", attrs.Key)
	require.Equal(t, int64(len(contents[1])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
	}

	attrsList, err := CopyObjects(options)
	require.NoError(t, err)

	for i, attrs := range attrsList {
		require.Equal(t, int64(len(contents[i])), *attrs.Size)
		require.NotEmpty(t, attrs.ETag)
		require.True(t, time.Now().After(*attrs.LastModified))
	}

	dst1, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})
	require.NoError(t, err)
	require.Equal(t, contents[0], testutil.ReadAll(t, dst1.Body))

	dst2, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key2",
	})
	require.NoError(t, err)
	require.Equal(t, contents[1], testutil.ReadAll(t, dst2.Body))
}

func TestCopyObjectsWithDelimiter(t *testing.T) {
	var (
		client   = objcli.NewTestClient(t, objval.ProviderAWS)
		contents = [][]byte{[]byte("1"), []byte("2")}
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(contents[0]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key1", attrs.Key)
	require.Equal(t, int64(len(contents[0])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	attrs, err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/skip/key2",
		Body:   bytes.NewReader(contents[1]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/skip/key2", attrs.Key)
	require.Equal(t, int64(len(contents[1])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst/",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src/",
		SourceDelimiter:   "/",
	}

	attrsList, err := CopyObjects(options)
	require.NoError(t, err)

	for i, attrs := range attrsList {
		require.Equal(t, int64(len(contents[i])), *attrs.Size)
		require.NotEmpty(t, attrs.ETag)
		require.True(t, time.Now().After(*attrs.LastModified))
	}

	dst1, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})
	require.NoError(t, err)
	require.Equal(t, contents[0], testutil.ReadAll(t, dst1.Body))

	_, err = client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/skip/key2",
	})

	var notFound *objerr.NotFoundError

	require.ErrorAs(t, err, &notFound)
}

func TestCopyObjectsWithInclude(t *testing.T) {
	var (
		client   = objcli.NewTestClient(t, objval.ProviderAWS)
		contents = [][]byte{[]byte("1"), []byte("2")}
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(contents[0]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key1", attrs.Key)
	require.Equal(t, int64(len(contents[0])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	attrs, err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key2",
		Body:   bytes.NewReader(contents[1]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key2", attrs.Key)
	require.Equal(t, int64(len(contents[1])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
		SourceInclude:     []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("src/key1"))},
	}

	attrsList, err := CopyObjects(options)
	require.NoError(t, err)

	for i, attrs := range attrsList {
		require.Equal(t, int64(len(contents[i])), *attrs.Size)
		require.NotEmpty(t, attrs.ETag)
		require.True(t, time.Now().After(*attrs.LastModified))
	}

	dst1, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})
	require.NoError(t, err)
	require.Equal(t, contents[0], testutil.ReadAll(t, dst1.Body))

	_, err = client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key2",
	})

	var notFound *objerr.NotFoundError

	require.ErrorAs(t, err, &notFound)
}

func TestCopyObjectsWithExclude(t *testing.T) {
	var (
		client   = objcli.NewTestClient(t, objval.ProviderAWS)
		contents = [][]byte{[]byte("1"), []byte("2")}
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(contents[0]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key1", attrs.Key)
	require.Equal(t, int64(len(contents[0])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	attrs, err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key2",
		Body:   bytes.NewReader(contents[1]),
	})
	require.NoError(t, err)

	require.Equal(t, "src/key2", attrs.Key)
	require.Equal(t, int64(len(contents[1])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
		SourceExclude:     []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("src/key1"))},
	}

	attrsList, err := CopyObjects(options)
	require.NoError(t, err)

	attrs = attrsList[0]

	require.Equal(t, "dst/key2", attrs.Key)
	require.Equal(t, int64(len(contents[0])), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	_, err = client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})

	var notFound *objerr.NotFoundError

	require.ErrorAs(t, err, &notFound)

	dst2, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key2",
	})
	require.NoError(t, err)
	require.Equal(t, contents[1], testutil.ReadAll(t, dst2.Body))
}

func TestCopyObjectsSingleObject(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		body   = []byte("Hello, World!")
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "srcKey",
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)

	require.Equal(t, "srcKey", attrs.Key)
	require.Equal(t, int64(len(body)), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "srcBucket",
		DestinationPrefix: "dstKey",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "srcKey",
	}

	attrsList, err := CopyObjects(options)
	require.NoError(t, err)

	attrs = attrsList[0]

	require.Equal(t, "dstKey", attrs.Key)
	require.Equal(t, int64(len(body)), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	dst, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "srcBucket",
		Key:    "dstKey",
	})
	require.NoError(t, err)
	require.Equal(t, body, testutil.ReadAll(t, dst.Body))
}
