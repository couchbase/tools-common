package objutil

import (
	"bytes"
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	testutil "github.com/couchbase/tools-common/testing/util"
)

func TestCopyObjectsSamePrefix(t *testing.T) {
	options := CopyObjectsOptions{
		DestinationPrefix: "prefix",
		SourcePrefix:      "prefix",
	}

	err := CopyObjects(options)
	require.ErrorIs(t, err, ErrCopyToSamePrefix)
}

func TestCopyObjects(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		body1  = []byte("1")
		body2  = []byte("2")
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(body1),
	})
	require.NoError(t, err)

	err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key2",
		Body:   bytes.NewReader(body2),
	})
	require.NoError(t, err)

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
	}

	err = CopyObjects(options)
	require.NoError(t, err)

	dst1, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})
	require.NoError(t, err)
	require.Equal(t, body1, testutil.ReadAll(t, dst1.Body))

	dst2, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key2",
	})
	require.NoError(t, err)
	require.Equal(t, body2, testutil.ReadAll(t, dst2.Body))
}

func TestCopyObjectsWithDelimiter(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		body1  = []byte("1")
		body2  = []byte("2")
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(body1),
	})
	require.NoError(t, err)

	err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/skip/key2",
		Body:   bytes.NewReader(body2),
	})
	require.NoError(t, err)

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
		SourceDelimiter:   "/",
	}

	err = CopyObjects(options)
	require.NoError(t, err)

	dst1, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})
	require.NoError(t, err)
	require.Equal(t, body1, testutil.ReadAll(t, dst1.Body))

	_, err = client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/skip/key2",
	})

	var notFound *objerr.NotFoundError

	require.ErrorAs(t, err, &notFound)
}

func TestCopyObjectsWithInclude(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		body1  = []byte("1")
		body2  = []byte("2")
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(body1),
	})
	require.NoError(t, err)

	err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key2",
		Body:   bytes.NewReader(body2),
	})
	require.NoError(t, err)

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
		SourceInclude:     []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("src/key1"))},
	}

	err = CopyObjects(options)
	require.NoError(t, err)

	dst1, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key1",
	})
	require.NoError(t, err)
	require.Equal(t, body1, testutil.ReadAll(t, dst1.Body))

	_, err = client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "dstBucket",
		Key:    "dst/key2",
	})

	var notFound *objerr.NotFoundError

	require.ErrorAs(t, err, &notFound)
}

func TestCopyObjectsWithExclude(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		body1  = []byte("1")
		body2  = []byte("2")
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key1",
		Body:   bytes.NewReader(body1),
	})
	require.NoError(t, err)

	err = client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "src/key2",
		Body:   bytes.NewReader(body2),
	})
	require.NoError(t, err)

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "dstBucket",
		DestinationPrefix: "dst",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "src",
		SourceExclude:     []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("src/key1"))},
	}

	err = CopyObjects(options)
	require.NoError(t, err)

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
	require.Equal(t, body2, testutil.ReadAll(t, dst2.Body))
}

func TestCopyObjectsSingleObject(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		body   = []byte("Hello, World!")
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "srcBucket",
		Key:    "srcKey",
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)

	options := CopyObjectsOptions{
		Client:            client,
		DestinationBucket: "srcBucket",
		DestinationPrefix: "dstKey",
		SourceBucket:      "srcBucket",
		SourcePrefix:      "srcKey",
	}

	err = CopyObjects(options)
	require.NoError(t, err)

	dst, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "srcBucket",
		Key:    "dstKey",
	})
	require.NoError(t, err)
	require.Equal(t, body, testutil.ReadAll(t, dst.Body))
}
