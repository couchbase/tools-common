package objaws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/tools-common/cloud/v5/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v5/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v5/objstore/objval"
	"github.com/couchbase/tools-common/testing/mock/matchers"
	testutil "github.com/couchbase/tools-common/testing/util"
	"github.com/couchbase/tools-common/types/ptr"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	var (
		api    = &mockServiceAPI{}
		logger = slog.Default()
	)

	require.Equal(t, &Client{serviceAPI: api, logger: logger}, NewClient(ClientOptions{ServiceAPI: api}))
}

func TestClientProvider(t *testing.T) {
	require.Equal(t, objval.ProviderAWS, (&Client{}).Provider())
}

func TestClientGetObject(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.GetObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output := &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("value")),
		ContentLength: ptr.To(int64(len("value"))),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("GetObject", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	object, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "bucket",
		Key:    "key",
	})
	require.NoError(t, err)

	require.Equal(t, []byte("value"), testutil.ReadAll(t, object.Body))
	object.Body = nil

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "key",
			Size:         ptr.To(int64(len("value"))),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
	}

	require.Equal(t, expected, object)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "GetObject", 1)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.GetObjectInput) bool {
		return input.Range != nil && *input.Range == "bytes=64-128"
	}

	output := &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("value")),
		ContentLength: ptr.To(int64(len("value"))),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("GetObject", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	_, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket:    "bucket",
		Key:       "key",
		ByteRange: &objval.ByteRange{Start: 64, End: 128},
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "GetObject", 1)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket:    "bucket",
		Key:       "key",
		ByteRange: &objval.ByteRange{Start: 128, End: 64},
	})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientGetObjectAttrs(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.HeadObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output := &s3.HeadObjectOutput{
		ETag:          ptr.To("etag"),
		ContentLength: ptr.To[int64](5),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObject", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	attrs, err := client.GetObjectAttrs(context.Background(), objcli.GetObjectAttrsOptions{
		Bucket: "bucket",
		Key:    "key",
	})
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "key",
		ETag:         ptr.To("etag"),
		Size:         ptr.To[int64](5),
		LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	require.Equal(t, expected, attrs)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObject", 1)
}

func TestClientPutObject(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.PutObjectInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("value"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return body && bucket && key
	}

	api.On("PutObject", matchers.Context, mock.MatchedBy(fn)).Return(&s3.PutObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "PutObject", 1)
}

func TestClientAppendToObjectNotFound(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.HeadObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	api.On("HeadObject", matchers.Context, mock.MatchedBy(fn1)).
		Return(nil, &types.NoSuchKey{})

	fn2 := func(input *s3.PutObjectInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("appended"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return body && bucket && key
	}

	api.On("PutObject", matchers.Context, mock.MatchedBy(fn2)).Return(&s3.PutObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), objcli.AppendToObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("appended"),
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObject", 1)
	api.AssertNumberOfCalls(t, "PutObject", 1)
}

func TestClientCopyObject(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.CopyObjectInput) bool {
		var (
			bucket = ptr.From(input.Bucket) == "dstBucket"
			key    = ptr.From(input.Key) == "dstKey"
			source = ptr.From(input.CopySource) == url.PathEscape("srcBucket/srcKey")
		)

		return bucket && key && source
	}

	api.On("CopyObject", matchers.Context, mock.MatchedBy(fn1)).Return(&s3.CopyObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.CopyObject(context.Background(), objcli.CopyObjectOptions{
		DestinationBucket: "dstBucket",
		DestinationKey:    "dstKey",
		SourceBucket:      "srcBucket",
		SourceKey:         "srcKey",
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "CopyObject", 1)
}

func TestClientAppendToObjectDownloadAndAdd(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.HeadObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output1 := &s3.HeadObjectOutput{
		ETag:          ptr.To("etag"),
		ContentLength: ptr.To(int64(len("value"))),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObject", matchers.Context, mock.MatchedBy(fn1)).Return(output1, nil)

	fn2 := func(input *s3.GetObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output2 := &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("value")),
		ETag:          ptr.To("etag"),
		ContentLength: ptr.To(int64(len("value"))),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("GetObject", matchers.Context, mock.MatchedBy(fn2)).Return(output2, nil)

	fn3 := func(input *s3.PutObjectInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("valueappended"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return body && bucket && key
	}

	api.On("PutObject", matchers.Context, mock.MatchedBy(fn3)).Return(&s3.PutObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), objcli.AppendToObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("appended"),
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObject", 1)
	api.AssertNumberOfCalls(t, "GetObject", 1)
	api.AssertNumberOfCalls(t, "PutObject", 1)
}

func TestClientAppendToObjectCreateMPUThenCopyAndAppend(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.HeadObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output1 := &s3.HeadObjectOutput{
		ETag:          ptr.To("etag"),
		ContentLength: ptr.To[int64](MinUploadSize),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObject", matchers.Context, mock.MatchedBy(fn1)).Return(output1, nil)

	fn2 := func(input *s3.CreateMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output2 := &s3.CreateMultipartUploadOutput{
		UploadId: ptr.To("id"),
	}

	api.On("CreateMultipartUpload", matchers.Context, mock.MatchedBy(fn2)).Return(output2, nil)

	fn3 := func(input *s3.UploadPartCopyInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			src    = input.CopySource != nil && *input.CopySource == "bucket/key"
			rnge   = input.CopySourceRange != nil && *input.CopySourceRange == fmt.Sprintf("bytes=0-%d", MinUploadSize-1)
			key    = input.Key != nil && *input.Key == "key"
			number = input.PartNumber != nil && *input.PartNumber == 1
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && src && rnge && key && number && id
	}

	output3 := &s3.UploadPartCopyOutput{
		CopyPartResult: &types.CopyPartResult{ETag: ptr.To("etag1")},
	}

	api.On("UploadPartCopy", matchers.Context, mock.MatchedBy(fn3)).Return(output3, nil)

	fn4 := func(input *s3.UploadPartInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("appended"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			number = input.PartNumber != nil && *input.PartNumber == 2
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return body && bucket && key && number && id
	}

	output4 := &s3.UploadPartOutput{
		ETag: ptr.To("etag2"),
	}

	api.On("UploadPart", matchers.Context, mock.MatchedBy(fn4)).Return(output4, nil)

	fn5 := func(input *s3.CompleteMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
			parts  = reflect.DeepEqual(input.MultipartUpload.Parts, []types.CompletedPart{
				{ETag: ptr.To("etag1"), PartNumber: ptr.To[int32](1)},
				{ETag: ptr.To("etag2"), PartNumber: ptr.To[int32](2)},
			})
		)

		return bucket && key && id && parts
	}

	api.On("CompleteMultipartUpload", matchers.Context, mock.MatchedBy(fn5)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), objcli.AppendToObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("appended"),
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObject", 1)
	api.AssertNumberOfCalls(t, "CreateMultipartUpload", 1)
	api.AssertNumberOfCalls(t, "UploadPartCopy", 1)
	api.AssertNumberOfCalls(t, "UploadPart", 1)
	api.AssertNumberOfCalls(t, "CompleteMultipartUpload", 1)
}

func TestClientAppendToObjectCreateMPUThenCopyAndAppendAbortOnFailure(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.HeadObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output1 := &s3.HeadObjectOutput{
		ETag:          ptr.To("etag"),
		ContentLength: ptr.To[int64](MinUploadSize),
		LastModified:  ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObject", matchers.Context, mock.MatchedBy(fn1)).Return(output1, nil)

	fn2 := func(input *s3.CreateMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output2 := &s3.CreateMultipartUploadOutput{
		UploadId: ptr.To("id"),
	}

	api.On("CreateMultipartUpload", matchers.Context, mock.MatchedBy(fn2)).Return(output2, nil)

	fn3 := func(input *s3.UploadPartCopyInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			src    = input.CopySource != nil && *input.CopySource == "bucket/key"
			rnge   = input.CopySourceRange != nil && *input.CopySourceRange == fmt.Sprintf("bytes=0-%d", MinUploadSize-1)
			key    = input.Key != nil && *input.Key == "key"
			number = input.PartNumber != nil && *input.PartNumber == 1
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && src && rnge && key && number && id
	}

	api.On("UploadPartCopy", matchers.Context, mock.MatchedBy(fn3)).Return(nil, assert.AnError)

	fn4 := func(input *s3.AbortMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && key && id
	}

	api.On("AbortMultipartUpload", matchers.Context, mock.MatchedBy(fn4)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), objcli.AppendToObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("appended"),
	})
	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObject", 1)
	api.AssertNumberOfCalls(t, "CreateMultipartUpload", 1)
	api.AssertNumberOfCalls(t, "UploadPartCopy", 1)
	api.AssertNumberOfCalls(t, "AbortMultipartUpload", 1)
}

func TestClientDeleteObjectsNoKeys(t *testing.T) {
	api := &mockServiceAPI{}
	client := &Client{serviceAPI: api}

	require.Equal(t, nil, client.deleteObjects(context.Background(), "bucket"))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjects", 0)
}

func TestClientDeleteObjectsSinglePage(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.DeleteObjectsInput) bool {
		var (
			bucket  = input.Bucket != nil && *input.Bucket == "bucket"
			quiet   = input.Delete != nil && input.Delete.Quiet != nil && *input.Delete.Quiet
			objects = input.Delete != nil && reflect.DeepEqual(input.Delete.Objects, []types.ObjectIdentifier{
				{Key: ptr.To("key1")},
				{Key: ptr.To("key2")},
				{Key: ptr.To("key3")},
			})
		)

		return bucket && quiet && objects
	}

	api.On("DeleteObjects", matchers.Context, mock.MatchedBy(fn)).
		Return(&s3.DeleteObjectsOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.DeleteObjects(context.Background(), objcli.DeleteObjectsOptions{
		Bucket: "bucket",
		Keys:   []string{"key1", "key2", "key3"},
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjects", 1)
}

func TestClientDeleteObjectsMultiplePages(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.DeleteObjectsInput) bool {
		var (
			bucket  = input.Bucket != nil && *input.Bucket == "bucket"
			quiet   = input.Delete != nil && input.Delete.Quiet != nil && *input.Delete.Quiet
			objects = input.Delete != nil && len(input.Delete.Objects) == PageSize
		)

		return bucket && quiet && objects
	}

	api.On("DeleteObjects", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.DeleteObjectsOutput{}, nil)

	fn2 := func(input *s3.DeleteObjectsInput) bool {
		var (
			bucket  = input.Bucket != nil && *input.Bucket == "bucket"
			quiet   = input.Delete != nil && input.Delete.Quiet != nil && *input.Delete.Quiet
			objects = input.Delete != nil && len(input.Delete.Objects) == 42
		)

		return bucket && quiet && objects
	}

	api.On("DeleteObjects", matchers.Context, mock.MatchedBy(fn2)).
		Return(&s3.DeleteObjectsOutput{}, nil)

	client := &Client{serviceAPI: api}

	keys := make([]string, 0, PageSize+42)

	for i := 0; i < PageSize+42; i++ {
		keys = append(keys, fmt.Sprintf("key%d", i))
	}

	err := client.DeleteObjects(context.Background(), objcli.DeleteObjectsOptions{
		Bucket: "bucket",
		Keys:   keys,
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjects", 2)
}

func TestClientDeleteObjectsIgnoreNotFoundError(t *testing.T) {
	api := &mockServiceAPI{}

	output := &s3.DeleteObjectsOutput{
		Errors: []types.Error{{Code: ptr.To("NoSuchKey"), Message: ptr.To("")}},
	}

	api.On("DeleteObjects", matchers.Context, mock.Anything).Return(output, nil)

	client := &Client{serviceAPI: api}

	err := client.DeleteObjects(context.Background(), objcli.DeleteObjectsOptions{
		Bucket: "bucket",
		Keys:   []string{"key"},
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjects", 1)
}

func TestClientDeleteDirectory(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListObjectsV2Input) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			prefix = input.Prefix != nil && *input.Prefix == "prefix"
		)

		return bucket && prefix
	}

	contents1 := []types.Object{
		{
			Key:          ptr.To("/path/to/key1"),
			Size:         ptr.To[int64](64),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		{
			Key:          ptr.To("/path/to/key2"),
			Size:         ptr.To[int64](128),
			LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
		},
	}

	api.On("ListObjectsV2", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.ListObjectsV2Output{Contents: contents1}, nil)

	client := &Client{serviceAPI: api}

	callback := func(_ context.Context, bucket string, keys ...string) error {
		require.Equal(t, "bucket", bucket)
		require.Equal(t, []string{"/path/to/key1", "/path/to/key2"}, keys)

		return nil
	}

	err := client.deleteDirectory(context.Background(), "bucket", "prefix", callback)
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2", 1)
}

func TestClientDeleteDirectoryWithCallbackError(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListObjectsV2Input) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			prefix = input.Prefix != nil && *input.Prefix == "prefix"
		)

		return bucket && prefix
	}

	contents1 := []types.Object{
		{
			Key:          ptr.To("/path/to/key1"),
			Size:         ptr.To[int64](64),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		{
			Key:          ptr.To("/path/to/key2"),
			Size:         ptr.To[int64](128),
			LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
		},
	}

	api.On("ListObjectsV2", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.ListObjectsV2Output{Contents: contents1}, nil)

	client := &Client{serviceAPI: api}

	callback := func(_ context.Context, _ string, _ ...string) error {
		return assert.AnError
	}

	err := client.deleteDirectory(context.Background(), "bucket", "prefix", callback)
	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2", 1)
}

func TestClientDeleteDirectoryVersions(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListObjectVersionsInput) bool {
		bucket := input.Bucket != nil && *input.Bucket == "bucket"

		return bucket
	}

	contents1 := []types.ObjectVersion{
		{
			Key:          ptr.To("/path/to/key1"),
			VersionId:    ptr.To("0"),
			Size:         ptr.To[int64](64),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		{
			Key:          ptr.To("/path/to/key1"),
			VersionId:    ptr.To("1"),
			Size:         ptr.To[int64](64),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		{
			Key:          ptr.To("/path/to/key2"),
			Size:         ptr.To[int64](128),
			LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
		},
	}

	api.On("ListObjectVersions", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.ListObjectVersionsOutput{Versions: contents1}, nil)

	client := &Client{serviceAPI: api}

	callback := func(_ context.Context, bucket string, objects ...types.ObjectIdentifier) error {
		require.Equal(t, "bucket", bucket)

		expected := []types.ObjectIdentifier{
			{Key: ptr.To("/path/to/key1"), VersionId: ptr.To("0")},
			{Key: ptr.To("/path/to/key1"), VersionId: ptr.To("1")},
			{Key: ptr.To("/path/to/key2")},
		}
		require.Equal(t, expected, objects)

		return nil
	}

	err := client.deleteDirectoryVersions(context.Background(), "bucket", "", callback)
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectVersions", 1)
}

func TestClientEmptyBucketWithCallbackError(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListObjectVersionsInput) bool {
		bucket := input.Bucket != nil && *input.Bucket == "bucket"

		return bucket
	}

	contents1 := []types.ObjectVersion{
		{
			Key:          ptr.To("/path/to/key1"),
			Size:         ptr.To[int64](64),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		{
			Key:          ptr.To("/path/to/key2"),
			Size:         ptr.To[int64](128),
			LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
		},
	}

	api.On("ListObjectVersions", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.ListObjectVersionsOutput{Versions: contents1}, nil)

	client := &Client{serviceAPI: api}

	callback := func(_ context.Context, _ string, _ ...types.ObjectIdentifier) error {
		return assert.AnError
	}

	err := client.deleteDirectoryVersions(context.Background(), "bucket", "", callback)
	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectVersions", 1)
}

func TestClientIterateObjects(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.ListObjectsV2Input) bool {
		var (
			bucket    = input.Bucket != nil && *input.Bucket == "bucket"
			prefix    = input.Prefix != nil && *input.Prefix == "prefix"
			delimiter = input.Delimiter != nil && *input.Delimiter == "delimiter"
		)

		return bucket && prefix && delimiter
	}

	api.On("ListObjectsV2", matchers.Context, mock.MatchedBy(fn)).
		Return(&s3.ListObjectsV2Output{}, nil)

	client := &Client{serviceAPI: api}

	err := client.IterateObjects(context.Background(), objcli.IterateObjectsOptions{
		Bucket:    "bucket",
		Prefix:    "prefix",
		Delimiter: "delimiter",
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2", 1)
}

func TestClientIterateObjectsBothIncludeExcludeSupplied(t *testing.T) {
	client := &Client{}

	err := client.IterateObjects(context.Background(), objcli.IterateObjectsOptions{
		Include: []*regexp.Regexp{},
		Exclude: []*regexp.Regexp{},
	})
	require.ErrorIs(t, err, objcli.ErrIncludeAndExcludeAreMutuallyExclusive)
}

func TestClientIterateObjectsDirectoryStub(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListObjectsV2Input) bool {
		var (
			bucket    = input.Bucket != nil && *input.Bucket == "bucket"
			prefix    = input.Prefix != nil && *input.Prefix == "prefix"
			delimiter = input.Delimiter != nil && *input.Delimiter == "delimiter"
		)

		return bucket && prefix && delimiter
	}

	output1 := &s3.ListObjectsV2Output{
		CommonPrefixes: []types.CommonPrefix{
			{
				Prefix: ptr.To("/path/to/key1"),
			},
		},
		Contents: []types.Object{
			{
				Key:          ptr.To("/path/to/key1"),
				Size:         ptr.To[int64](64),
				LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
			},
		},
	}

	api.On("ListObjectsV2", matchers.Context, mock.MatchedBy(fn1)).
		Return(output1, nil)

	var (
		client  = &Client{serviceAPI: api}
		dirs    int
		objects int
	)

	fn := func(attrs *objval.ObjectAttrs) error {
		if attrs.IsDir() {
			dirs++
		} else {
			objects++
		}

		return nil
	}

	err := client.IterateObjects(context.Background(), objcli.IterateObjectsOptions{
		Bucket:    "bucket",
		Prefix:    "prefix",
		Delimiter: "delimiter",
		Func:      fn,
	})
	require.NoError(t, err)
	require.Equal(t, 1, dirs)
	require.Equal(t, 1, objects)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2", 1)
}

func TestClientIterateObjectsPropagateUserError(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListObjectsV2Input) bool {
		var (
			bucket    = input.Bucket != nil && *input.Bucket == "bucket"
			prefix    = input.Prefix != nil && *input.Prefix == "prefix"
			delimiter = input.Delimiter != nil && *input.Delimiter == "delimiter"
		)

		return bucket && prefix && delimiter
	}

	contents1 := []types.Object{
		{
			Key:          ptr.To("/path/to/key1"),
			Size:         ptr.To[int64](64),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
	}

	api.On("ListObjectsV2", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.ListObjectsV2Output{Contents: contents1}, nil)

	client := &Client{serviceAPI: api}

	err := client.IterateObjects(context.Background(), objcli.IterateObjectsOptions{
		Bucket:    "bucket",
		Prefix:    "prefix",
		Delimiter: "delimiter",
		Func:      func(_ *objval.ObjectAttrs) error { return assert.AnError },
	})
	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2", 1)
}

func TestClientIterateObjectsWithIncludeExclude(t *testing.T) {
	type test struct {
		name             string
		include, exclude []*regexp.Regexp
		all              []*objval.ObjectAttrs
	}

	tests := []*test{
		{
			name: "All",
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key1",
					Size:         ptr.To[int64](64),
					LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         ptr.To[int64](128),
					LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
				},
				{
					Key:          "/path/to/key2",
					Size:         ptr.To[int64](256),
					LastModified: ptr.To((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
		{
			name:    "IncludeWithFullPath",
			include: []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("/path/to/key1"))},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key1",
					Size:         ptr.To[int64](64),
					LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
				},
			},
		},
		{
			name:    "IncludeWithBasename",
			include: []*regexp.Regexp{regexp.MustCompile("key1")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key1",
					Size:         ptr.To[int64](64),
					LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         ptr.To[int64](128),
					LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
				},
			},
		},
		{
			name:    "IncludeMustMatchBasename",
			include: []*regexp.Regexp{regexp.MustCompile("^key1$")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key1",
					Size:         ptr.To[int64](64),
					LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         ptr.To[int64](128),
					LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
				},
			},
		},
		{
			name:    "ExcludeWithFullPath",
			exclude: []*regexp.Regexp{regexp.MustCompile("key1")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key2",
					Size:         ptr.To[int64](256),
					LastModified: ptr.To((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
		{
			name:    "ExcludeWithBasename",
			exclude: []*regexp.Regexp{regexp.MustCompile("key1")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key2",
					Size:         ptr.To[int64](256),
					LastModified: ptr.To((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
		{
			name:    "ExcludeMustMatchBasename",
			exclude: []*regexp.Regexp{regexp.MustCompile("^key1$")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/key2",
					Size:         ptr.To[int64](256),
					LastModified: ptr.To((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := &mockServiceAPI{}

			fn1 := func(input *s3.ListObjectsV2Input) bool {
				var (
					bucket = input.Bucket != nil && *input.Bucket == "bucket"
					prefix = input.Prefix != nil && *input.Prefix == "prefix"
				)

				return bucket && prefix
			}

			contents1 := []types.Object{
				{
					Key:          ptr.To("/path/to/key1"),
					Size:         ptr.To[int64](64),
					LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          ptr.To("/path/to/another/key1"),
					Size:         ptr.To[int64](128),
					LastModified: ptr.To((time.Time{}).Add(48 * time.Hour)),
				},
				{
					Key:          ptr.To("/path/to/key2"),
					Size:         ptr.To[int64](256),
					LastModified: ptr.To((time.Time{}).Add(72 * time.Hour)),
				},
			}

			api.On("ListObjectsV2", matchers.Context, mock.MatchedBy(fn1)).
				Return(&s3.ListObjectsV2Output{Contents: contents1}, nil)

			client := &Client{serviceAPI: api}

			var all []*objval.ObjectAttrs

			fn := func(attrs *objval.ObjectAttrs) error { all = append(all, attrs); return nil }

			err := client.IterateObjects(context.Background(), objcli.IterateObjectsOptions{
				Bucket:    "bucket",
				Prefix:    "prefix",
				Delimiter: "delimiter",
				Include:   test.include,
				Exclude:   test.exclude,
				Func:      fn,
			})
			require.NoError(t, err)
			require.Equal(t, test.all, all)

			api.AssertExpectations(t)
			api.AssertNumberOfCalls(t, "ListObjectsV2", 1)
		})
	}
}

func TestClientCreateMultipartUpload(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.CreateMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output := &s3.CreateMultipartUploadOutput{
		UploadId: ptr.To("id"),
	}

	api.On("CreateMultipartUpload", matchers.Context, mock.MatchedBy(fn), mock.Anything).
		Return(output, nil)

	client := &Client{serviceAPI: api}

	id, err := client.CreateMultipartUpload(context.Background(), objcli.CreateMultipartUploadOptions{
		Bucket: "bucket",
		Key:    "key",
	})
	require.NoError(t, err)
	require.Equal(t, "id", id)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "CreateMultipartUpload", 1)
}

func TestClientListParts(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListPartsInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			id     = input.UploadId != nil && *input.UploadId == "id"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && id && key
	}

	parts1 := []types.Part{
		{
			ETag: ptr.To("etag1"),
			Size: ptr.To[int64](64),
		},
		{
			ETag: ptr.To("etag2"),
			Size: ptr.To[int64](128),
		},
	}

	api.On("ListParts", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.ListPartsOutput{Parts: parts1}, nil)

	client := &Client{serviceAPI: api}

	parts, err := client.ListParts(context.Background(), objcli.ListPartsOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "key",
	})
	require.NoError(t, err)

	expected := []objval.Part{{ID: "etag1", Size: 64}, {ID: "etag2", Size: 128}}
	require.Equal(t, expected, parts)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListParts", 1)
}

func TestClientListPartsUploadNotFound(t *testing.T) {
	api := &mockServiceAPI{}

	fn1 := func(input *s3.ListPartsInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			id     = input.UploadId != nil && *input.UploadId == "id"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && id && key
	}

	api.On("ListParts", matchers.Context, mock.MatchedBy(fn1)).
		Return(nil, &types.NoSuchUpload{})

	client := &Client{serviceAPI: api}

	_, err := client.ListParts(context.Background(), objcli.ListPartsOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "key",
	})
	require.True(t, objerr.IsNotFoundError(err))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListParts", 1)
}

func TestClientUploadPart(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.UploadPartInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("value"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			number = input.PartNumber != nil && *input.PartNumber == 1
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return body && bucket && key && number && id
	}

	output := &s3.UploadPartOutput{
		ETag: ptr.To("etag"),
	}

	api.On("UploadPart", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	part, err := client.UploadPart(context.Background(), objcli.UploadPartOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "key",
		Number:   1,
		Body:     strings.NewReader("value"),
	})
	require.NoError(t, err)
	require.Equal(t, objval.Part{ID: "etag", Number: 1, Size: 5}, part)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "UploadPart", 1)
}

func TestClientUploadPartCopy(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.UploadPartCopyInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "dstBucket"
			src    = input.CopySource != nil && *input.CopySource == "srcBucket/key1"
			rnge   = input.CopySourceRange != nil && *input.CopySourceRange == "bytes=64-128"
			key    = input.Key != nil && *input.Key == "key2"
			number = input.PartNumber != nil && *input.PartNumber == 1
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && src && rnge && key && number && id
	}

	output := &s3.UploadPartCopyOutput{
		CopyPartResult: &types.CopyPartResult{ETag: ptr.To("etag")},
	}

	api.On("UploadPartCopy", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	part, err := client.UploadPartCopy(context.Background(), objcli.UploadPartCopyOptions{
		DestinationBucket: "dstBucket",
		UploadID:          "id",
		DestinationKey:    "key2",
		SourceBucket:      "srcBucket",
		SourceKey:         "key1",
		Number:            1,
		ByteRange:         &objval.ByteRange{Start: 64, End: 128},
	})
	require.NoError(t, err)
	require.Equal(t, objval.Part{ID: "etag", Number: 1, Size: 65}, part)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "UploadPartCopy", 1)
}

func TestClientUploadPartCopyInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy(context.Background(), objcli.UploadPartCopyOptions{
		DestinationBucket: "dstBucket",
		UploadID:          "id",
		DestinationKey:    "key2",
		SourceBucket:      "srcBucket",
		SourceKey:         "key1",
		Number:            1,
		ByteRange:         &objval.ByteRange{Start: 128, End: 64},
	})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientCompleteMultipartUpload(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.CompleteMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
			parts  = reflect.DeepEqual(input.MultipartUpload.Parts, []types.CompletedPart{
				{ETag: ptr.To("etag1"), PartNumber: ptr.To[int32](1)},
				{ETag: ptr.To("etag2"), PartNumber: ptr.To[int32](2)},
			})
		)

		return bucket && key && id && parts
	}

	api.On("CompleteMultipartUpload", matchers.Context, mock.MatchedBy(fn)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	err := client.CompleteMultipartUpload(context.Background(), objcli.CompleteMultipartUploadOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "key",
		Parts:    []objval.Part{{ID: "etag1", Number: 1}, {ID: "etag2", Number: 2}},
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "CompleteMultipartUpload", 1)
}

func TestClientAbortMultipartUpload(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.AbortMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && key && id
	}

	api.On("AbortMultipartUpload", matchers.Context, mock.MatchedBy(fn)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	err := client.AbortMultipartUpload(context.Background(), objcli.AbortMultipartUploadOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "key",
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "AbortMultipartUpload", 1)
}

func TestClientAbortMultipartUploadNoSuchUpload(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.AbortMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && key && id
	}

	api.On("AbortMultipartUpload", matchers.Context, mock.MatchedBy(fn)).
		Return(nil, &types.NoSuchUpload{})

	client := &Client{serviceAPI: api}

	err := client.AbortMultipartUpload(context.Background(), objcli.AbortMultipartUploadOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "key",
	})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "AbortMultipartUpload", 1)
}
