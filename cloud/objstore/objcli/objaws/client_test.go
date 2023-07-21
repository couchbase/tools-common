package objaws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/tools-common/cloud/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/objstore/objval"
	"github.com/couchbase/tools-common/testing/mock/matchers"
	testutil "github.com/couchbase/tools-common/testing/util"
	"github.com/couchbase/tools-common/types/ptr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockError struct{ inner string }

func (m *mockError) Error() string   { return m.inner }
func (m *mockError) String() string  { return m.inner }
func (m *mockError) Code() string    { return m.inner }
func (m *mockError) Message() string { return m.inner }
func (m *mockError) OrigErr() error  { return nil }

func TestNewClient(t *testing.T) {
	api := &mockServiceAPI{}

	require.Equal(t, &Client{serviceAPI: api}, NewClient(ClientOptions{api}))
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
		ContentLength: aws.Int64(int64(len("value"))),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("GetObjectWithContext", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	object, err := client.GetObject(context.Background(), "bucket", "key", nil)
	require.NoError(t, err)

	require.Equal(t, []byte("value"), testutil.ReadAll(t, object.Body))
	object.Body = nil

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "key",
			Size:         ptr.To(int64(len("value"))),
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
	}

	require.Equal(t, expected, object)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "GetObjectWithContext", 1)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.GetObjectInput) bool {
		return input.Range != nil && *input.Range == "bytes=64-128"
	}

	output := &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("value")),
		ContentLength: aws.Int64(int64(len("value"))),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("GetObjectWithContext", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	_, err := client.GetObject(context.Background(), "bucket", "key", &objval.ByteRange{Start: 64, End: 128})
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "GetObjectWithContext", 1)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject(context.Background(), "bucket", "key", &objval.ByteRange{Start: 128, End: 64})

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
		ETag:          aws.String("etag"),
		ContentLength: aws.Int64(5),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObjectWithContext", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	attrs, err := client.GetObjectAttrs(context.Background(), "bucket", "key")
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "key",
		ETag:         ptr.To("etag"),
		Size:         ptr.To[int64](5),
		LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	require.Equal(t, expected, attrs)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObjectWithContext", 1)
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

	api.On("PutObjectWithContext", matchers.Context, mock.MatchedBy(fn)).Return(&s3.PutObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	require.NoError(t, client.PutObject(context.Background(), "bucket", "key", strings.NewReader("value")))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "PutObjectWithContext", 1)
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

	api.On("HeadObjectWithContext", matchers.Context, mock.MatchedBy(fn1)).
		Return(nil, &mockError{s3.ErrCodeNoSuchKey})

	fn2 := func(input *s3.PutObjectInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("appended"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return body && bucket && key
	}

	api.On("PutObjectWithContext", matchers.Context, mock.MatchedBy(fn2)).Return(&s3.PutObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), "bucket", "key", strings.NewReader("appended"))
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObjectWithContext", 1)
	api.AssertNumberOfCalls(t, "PutObjectWithContext", 1)
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
		ETag:          aws.String("etag"),
		ContentLength: aws.Int64(int64(len("value"))),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObjectWithContext", matchers.Context, mock.MatchedBy(fn1)).Return(output1, nil)

	fn2 := func(input *s3.GetObjectInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output2 := &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("value")),
		ETag:          aws.String("etag"),
		ContentLength: aws.Int64(int64(len("value"))),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("GetObjectWithContext", matchers.Context, mock.MatchedBy(fn2)).Return(output2, nil)

	fn3 := func(input *s3.PutObjectInput) bool {
		var (
			body   = input.Body != nil && bytes.Equal(testutil.ReadAll(t, input.Body), []byte("valueappended"))
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return body && bucket && key
	}

	api.On("PutObjectWithContext", matchers.Context, mock.MatchedBy(fn3)).Return(&s3.PutObjectOutput{}, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), "bucket", "key", strings.NewReader("appended"))
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObjectWithContext", 1)
	api.AssertNumberOfCalls(t, "GetObjectWithContext", 1)
	api.AssertNumberOfCalls(t, "PutObjectWithContext", 1)
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
		ETag:          aws.String("etag"),
		ContentLength: aws.Int64(MinUploadSize),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObjectWithContext", matchers.Context, mock.MatchedBy(fn1)).Return(output1, nil)

	fn2 := func(input *s3.CreateMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output2 := &s3.CreateMultipartUploadOutput{
		UploadId: aws.String("id"),
	}

	api.On("CreateMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn2)).Return(output2, nil)

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
		CopyPartResult: &s3.CopyPartResult{ETag: aws.String("etag1")},
	}

	api.On("UploadPartCopyWithContext", matchers.Context, mock.MatchedBy(fn3)).Return(output3, nil)

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
		ETag: aws.String("etag2"),
	}

	api.On("UploadPartWithContext", matchers.Context, mock.MatchedBy(fn4)).Return(output4, nil)

	fn5 := func(input *s3.CompleteMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
			parts  = reflect.DeepEqual(input.MultipartUpload.Parts, []*s3.CompletedPart{
				{ETag: aws.String("etag1"), PartNumber: aws.Int64(1)},
				{ETag: aws.String("etag2"), PartNumber: aws.Int64(2)},
			})
		)

		return bucket && key && id && parts
	}

	api.On("CompleteMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn5)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), "bucket", "key", strings.NewReader("appended"))
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObjectWithContext", 1)
	api.AssertNumberOfCalls(t, "CreateMultipartUploadWithContext", 1)
	api.AssertNumberOfCalls(t, "UploadPartCopyWithContext", 1)
	api.AssertNumberOfCalls(t, "UploadPartWithContext", 1)
	api.AssertNumberOfCalls(t, "CompleteMultipartUploadWithContext", 1)
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
		ETag:          aws.String("etag"),
		ContentLength: aws.Int64(MinUploadSize),
		LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	api.On("HeadObjectWithContext", matchers.Context, mock.MatchedBy(fn1)).Return(output1, nil)

	fn2 := func(input *s3.CreateMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
		)

		return bucket && key
	}

	output2 := &s3.CreateMultipartUploadOutput{
		UploadId: aws.String("id"),
	}

	api.On("CreateMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn2)).Return(output2, nil)

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

	api.On("UploadPartCopyWithContext", matchers.Context, mock.MatchedBy(fn3)).Return(nil, assert.AnError)

	fn4 := func(input *s3.AbortMultipartUploadInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			key    = input.Key != nil && *input.Key == "key"
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && key && id
	}

	api.On("AbortMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn4)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	err := client.AppendToObject(context.Background(), "bucket", "key", strings.NewReader("appended"))
	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "HeadObjectWithContext", 1)
	api.AssertNumberOfCalls(t, "CreateMultipartUploadWithContext", 1)
	api.AssertNumberOfCalls(t, "UploadPartCopyWithContext", 1)
	api.AssertNumberOfCalls(t, "AbortMultipartUploadWithContext", 1)
}

func TestClientDeleteObjectsNoKeys(t *testing.T) {
	api := &mockServiceAPI{}
	client := &Client{serviceAPI: api}

	require.Equal(t, nil, client.deleteObjects(context.Background(), "bucket"))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjectsWithContext", 0)
}

func TestClientDeleteObjectsSinglePage(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.DeleteObjectsInput) bool {
		var (
			bucket  = input.Bucket != nil && *input.Bucket == "bucket"
			quiet   = input.Delete != nil && input.Delete.Quiet != nil && *input.Delete.Quiet
			objects = input.Delete != nil && reflect.DeepEqual(input.Delete.Objects, []*s3.ObjectIdentifier{
				{Key: aws.String("key1")},
				{Key: aws.String("key2")},
				{Key: aws.String("key3")},
			})
		)

		return bucket && quiet && objects
	}

	api.On("DeleteObjectsWithContext", matchers.Context, mock.MatchedBy(fn)).
		Return(&s3.DeleteObjectsOutput{}, nil)

	client := &Client{serviceAPI: api}

	require.NoError(t, client.DeleteObjects(context.Background(), "bucket", "key1", "key2", "key3"))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjectsWithContext", 1)
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

	api.On("DeleteObjectsWithContext", matchers.Context, mock.MatchedBy(fn1)).
		Return(&s3.DeleteObjectsOutput{}, nil)

	fn2 := func(input *s3.DeleteObjectsInput) bool {
		var (
			bucket  = input.Bucket != nil && *input.Bucket == "bucket"
			quiet   = input.Delete != nil && input.Delete.Quiet != nil && *input.Delete.Quiet
			objects = input.Delete != nil && len(input.Delete.Objects) == 42
		)

		return bucket && quiet && objects
	}

	api.On("DeleteObjectsWithContext", matchers.Context, mock.MatchedBy(fn2)).
		Return(&s3.DeleteObjectsOutput{}, nil)

	client := &Client{serviceAPI: api}

	keys := make([]string, 0, PageSize+42)

	for i := 0; i < PageSize+42; i++ {
		keys = append(keys, fmt.Sprintf("key%d", i))
	}

	require.NoError(t, client.DeleteObjects(context.Background(), "bucket", keys...))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjectsWithContext", 2)
}

func TestClientDeleteObjectsIgnoreNotFoundError(t *testing.T) {
	api := &mockServiceAPI{}

	output := &s3.DeleteObjectsOutput{
		Errors: []*s3.Error{{Code: aws.String(s3.ErrCodeNoSuchKey), Message: aws.String("")}},
	}

	api.On("DeleteObjectsWithContext", matchers.Context, mock.Anything).Return(output, nil)

	client := &Client{serviceAPI: api}

	require.NoError(t, client.DeleteObjects(context.Background(), "bucket", "key"))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "DeleteObjectsWithContext", 1)
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

	fn2 := func(fn func(page *s3.ListObjectsV2Output, _ bool) bool) bool {
		contents := []*s3.Object{
			{
				Key:          aws.String("/path/to/key1"),
				Size:         aws.Int64(64),
				LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
			},
			{
				Key:          aws.String("/path/to/key2"),
				Size:         aws.Int64(128),
				LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
			},
		}

		fn(&s3.ListObjectsV2Output{Contents: contents}, true)

		return true
	}

	api.On("ListObjectsV2PagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.MatchedBy(fn2)).
		Return(nil)

	client := &Client{serviceAPI: api}

	callback := func(_ context.Context, bucket string, keys ...string) error {
		require.Equal(t, "bucket", bucket)
		require.Equal(t, []string{"/path/to/key1", "/path/to/key2"}, keys)

		return nil
	}

	err := client.deleteDirectory(context.Background(), "bucket", "prefix", callback)
	require.NoError(t, err)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2PagesWithContext", 1)
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

	fn2 := func(fn func(page *s3.ListObjectsV2Output, _ bool) bool) bool {
		contents := []*s3.Object{
			{
				Key:          aws.String("/path/to/key1"),
				Size:         aws.Int64(64),
				LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
			},
			{
				Key:          aws.String("/path/to/key2"),
				Size:         aws.Int64(128),
				LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
			},
		}

		fn(&s3.ListObjectsV2Output{Contents: contents}, true)

		return true
	}

	api.On("ListObjectsV2PagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.MatchedBy(fn2)).
		Return(nil)

	client := &Client{serviceAPI: api}

	callback := func(_ context.Context, bucket string, keys ...string) error {
		return assert.AnError
	}

	err := client.deleteDirectory(context.Background(), "bucket", "prefix", callback)
	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2PagesWithContext", 1)
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

	api.On("ListObjectsV2PagesWithContext", matchers.Context, mock.MatchedBy(fn), mock.Anything).Return(nil)

	client := &Client{serviceAPI: api}

	require.NoError(t, client.IterateObjects(context.Background(), "bucket", "prefix", "delimiter", nil, nil, nil))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2PagesWithContext", 1)
}

func TestClientIterateObjectsBothIncludeExcludeSupplied(t *testing.T) {
	client := &Client{}

	err := client.IterateObjects(context.Background(), "bucket", "prefix", "delimiter", []*regexp.Regexp{},
		[]*regexp.Regexp{}, nil)
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

	fn2 := func(fn func(page *s3.ListObjectsV2Output, _ bool) bool) bool {
		fn(&s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{
					Prefix: aws.String("/path/to/key1"),
				},
			},
			Contents: []*s3.Object{
				{
					Key:          aws.String("/path/to/key1"),
					Size:         aws.Int64(64),
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
			},
		}, true)

		return true
	}

	api.On("ListObjectsV2PagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.MatchedBy(fn2)).
		Return(nil)

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

	require.NoError(t, client.IterateObjects(context.Background(), "bucket", "prefix", "delimiter", nil, nil, fn))
	require.Equal(t, 1, dirs)
	require.Equal(t, 1, objects)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2PagesWithContext", 1)
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

	fn2 := func(fn func(page *s3.ListObjectsV2Output, _ bool) bool) bool {
		fn(&s3.ListObjectsV2Output{Contents: []*s3.Object{
			{
				Key:          aws.String("/path/to/key1"),
				Size:         aws.Int64(64),
				LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
			},
		}}, true)

		return true
	}

	api.On("ListObjectsV2PagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.MatchedBy(fn2)).
		Return(nil)

	client := &Client{serviceAPI: api}

	err := client.IterateObjects(
		context.Background(),
		"bucket",
		"prefix",
		"delimiter",
		nil,
		nil,
		func(attrs *objval.ObjectAttrs) error {
			return assert.AnError
		},
	)

	require.ErrorIs(t, err, assert.AnError)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListObjectsV2PagesWithContext", 1)
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
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         ptr.To[int64](128),
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
				},
				{
					Key:          "/path/to/key2",
					Size:         ptr.To[int64](256),
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
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
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
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
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         ptr.To[int64](128),
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
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
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         ptr.To[int64](128),
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
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
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
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
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
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
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
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

			fn2 := func(fn func(page *s3.ListObjectsV2Output, _ bool) bool) bool {
				fn(&s3.ListObjectsV2Output{Contents: []*s3.Object{
					{
						Key:          aws.String("/path/to/key1"),
						Size:         aws.Int64(64),
						LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
					},
					{
						Key:          aws.String("/path/to/another/key1"),
						Size:         aws.Int64(128),
						LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
					},
					{
						Key:          aws.String("/path/to/key2"),
						Size:         aws.Int64(256),
						LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
					},
				}}, true)

				return true
			}

			api.On("ListObjectsV2PagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.MatchedBy(fn2)).
				Return(nil)

			client := &Client{serviceAPI: api}

			var all []*objval.ObjectAttrs

			fn := func(attrs *objval.ObjectAttrs) error { all = append(all, attrs); return nil }

			err := client.IterateObjects(context.Background(), "bucket", "prefix", "delimiter", test.include,
				test.exclude, fn)
			require.NoError(t, err)
			require.Equal(t, test.all, all)

			api.AssertExpectations(t)
			api.AssertNumberOfCalls(t, "ListObjectsV2PagesWithContext", 1)
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
		UploadId: aws.String("id"),
	}

	api.On("CreateMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn), mock.Anything).
		Return(output, nil)

	client := &Client{serviceAPI: api}

	id, err := client.CreateMultipartUpload(context.Background(), "bucket", "key")
	require.NoError(t, err)
	require.Equal(t, "id", id)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "CreateMultipartUploadWithContext", 1)
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

	fn2 := func(fn func(page *s3.ListPartsOutput, _ bool) bool) bool {
		parts := []*s3.Part{
			{
				ETag: aws.String("etag1"),
				Size: aws.Int64(64),
			},
			{
				ETag: aws.String("etag2"),
				Size: aws.Int64(128),
			},
		}

		fn(&s3.ListPartsOutput{Parts: parts}, false)

		return true
	}

	api.On("ListPartsPagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.MatchedBy(fn2)).Return(nil)

	client := &Client{serviceAPI: api}

	parts, err := client.ListParts(context.Background(), "bucket", "id", "key")
	require.NoError(t, err)

	expected := []objval.Part{{ID: "etag1", Size: 64}, {ID: "etag2", Size: 128}}
	require.Equal(t, expected, parts)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListPartsPagesWithContext", 1)
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

	api.On("ListPartsPagesWithContext", matchers.Context, mock.MatchedBy(fn1), mock.Anything).
		Return(&mockError{inner: s3.ErrCodeNoSuchUpload})

	client := &Client{serviceAPI: api}

	_, err := client.ListParts(context.Background(), "bucket", "id", "key")
	require.True(t, objerr.IsNotFoundError(err))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "ListPartsPagesWithContext", 1)
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
		ETag: aws.String("etag"),
	}

	api.On("UploadPartWithContext", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	part, err := client.UploadPart(context.Background(), "bucket", "id", "key", 1, strings.NewReader("value"))
	require.NoError(t, err)
	require.Equal(t, objval.Part{ID: "etag", Number: 1, Size: 5}, part)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "UploadPartWithContext", 1)
}

func TestClientUploadPartCopy(t *testing.T) {
	api := &mockServiceAPI{}

	fn := func(input *s3.UploadPartCopyInput) bool {
		var (
			bucket = input.Bucket != nil && *input.Bucket == "bucket"
			src    = input.CopySource != nil && *input.CopySource == "bucket/key2"
			rnge   = input.CopySourceRange != nil && *input.CopySourceRange == "bytes=64-128"
			key    = input.Key != nil && *input.Key == "key1"
			number = input.PartNumber != nil && *input.PartNumber == 1
			id     = input.UploadId != nil && *input.UploadId == "id"
		)

		return bucket && src && rnge && key && number && id
	}

	output := &s3.UploadPartCopyOutput{
		CopyPartResult: &s3.CopyPartResult{ETag: aws.String("etag")},
	}

	api.On("UploadPartCopyWithContext", matchers.Context, mock.MatchedBy(fn)).Return(output, nil)

	client := &Client{serviceAPI: api}

	part, err := client.UploadPartCopy(context.Background(), "bucket", "id", "key1", "key2", 1,
		&objval.ByteRange{Start: 64, End: 128})
	require.NoError(t, err)
	require.Equal(t, objval.Part{ID: "etag", Number: 1, Size: 65}, part)

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "UploadPartCopyWithContext", 1)
}

func TestClientUploadPartCopyInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy(context.Background(), "bucket", "id", "dst", "src", 1,
		&objval.ByteRange{Start: 128, End: 64})

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
			parts  = reflect.DeepEqual(input.MultipartUpload.Parts, []*s3.CompletedPart{
				{ETag: aws.String("etag1"), PartNumber: aws.Int64(1)},
				{ETag: aws.String("etag2"), PartNumber: aws.Int64(2)},
			})
		)

		return bucket && key && id && parts
	}

	api.On("CompleteMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	require.NoError(t, client.CompleteMultipartUpload(context.Background(),
		"bucket",
		"id",
		"key",
		objval.Part{ID: "etag1", Number: 1},
		objval.Part{ID: "etag2", Number: 2},
	))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "CompleteMultipartUploadWithContext", 1)
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

	api.On("AbortMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn)).Return(nil, nil)

	client := &Client{serviceAPI: api}

	require.NoError(t, client.AbortMultipartUpload(context.Background(), "bucket", "id", "key"))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "AbortMultipartUploadWithContext", 1)
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

	api.On("AbortMultipartUploadWithContext", matchers.Context, mock.MatchedBy(fn)).
		Return(nil, &mockError{inner: s3.ErrCodeNoSuchUpload})

	client := &Client{serviceAPI: api}

	require.NoError(t, client.AbortMultipartUpload(context.Background(), "bucket", "id", "key"))

	api.AssertExpectations(t)
	api.AssertNumberOfCalls(t, "AbortMultipartUploadWithContext", 1)
}
