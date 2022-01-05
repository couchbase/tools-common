package objgcp

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func TestNewClient(t *testing.T) {
	require.Equal(t, &Client{serviceAPI: &serviceClient{c: &storage.Client{}}}, NewClient(&storage.Client{}))
}

func TestClientProvider(t *testing.T) {
	require.Equal(t, objval.ProviderGCP, (&Client{}).Provider())
}

func TestClientGetObject(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
		mrAPI = &mockReaderAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return key == "key" })).Return(moAPI)

	moAPI.On(
		"NewRangeReader",
		mock.Anything,
		mock.MatchedBy(func(offset int64) bool { return offset == 0 }),
		mock.MatchedBy(func(length int64) bool { return length == -1 }),
	).Return(mrAPI, nil)

	output := storage.ReaderObjectAttrs{
		Size:         42,
		LastModified: (time.Time{}).Add(24 * time.Hour),
	}

	mrAPI.On("Attrs", mock.Anything).Return(output, nil)

	client := &Client{serviceAPI: msAPI}

	object, err := client.GetObject("bucket", "key", nil)
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "key",
			Size:         42,
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
		Body: mrAPI,
	}

	require.Equal(t, expected, object)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 1)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "NewRangeReader", 1)

	mrAPI.AssertExpectations(t)
	mrAPI.AssertNumberOfCalls(t, "Attrs", 1)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
		mrAPI = &mockReaderAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return key == "key" })).Return(moAPI)

	moAPI.On(
		"NewRangeReader",
		mock.Anything,
		mock.MatchedBy(func(offset int64) bool { return offset == 64 }),
		mock.MatchedBy(func(length int64) bool { return length == 65 }),
	).Return(mrAPI, nil)

	output := storage.ReaderObjectAttrs{
		Size:         64,
		LastModified: (time.Time{}).Add(24 * time.Hour),
	}

	mrAPI.On("Attrs", mock.Anything).Return(output, nil)

	client := &Client{serviceAPI: msAPI}

	object, err := client.GetObject("bucket", "key", &objval.ByteRange{Start: 64, End: 128})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "key",
			Size:         64,
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
		Body: mrAPI,
	}

	require.Equal(t, expected, object)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 1)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "NewRangeReader", 1)

	mrAPI.AssertExpectations(t)
	mrAPI.AssertNumberOfCalls(t, "Attrs", 1)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject("bucket", "key", &objval.ByteRange{Start: 128, End: 64})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientGetObjectAttrs(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return key == "key" })).Return(moAPI)

	output := &storage.ObjectAttrs{
		Name:    "key",
		Etag:    "etag",
		Size:    5,
		Updated: (time.Time{}).Add(24 * time.Hour),
	}

	moAPI.On("Attrs", mock.Anything).Return(output, nil)

	client := &Client{serviceAPI: msAPI}

	attrs, err := client.GetObjectAttrs("bucket", "key")
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "key",
		ETag:         "etag",
		Size:         5,
		LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	require.Equal(t, expected, attrs)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 1)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "Attrs", 1)
}

func TestClientPutObject(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
		mwAPI = &mockWriterAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return key == "key" })).Return(moAPI)

	moAPI.On("NewWriter", mock.Anything).Return(mwAPI, nil)

	fn1 := func(sum []byte) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(sum, expected[:])
	}

	mwAPI.On("SendMD5", mock.MatchedBy(fn1))

	fn2 := func(sum uint32) bool {
		hasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		hasher.Write([]byte("value"))

		return sum == hasher.Sum32()
	}

	mwAPI.On("SendCRC", mock.MatchedBy(fn2))

	fn3 := func(data []byte) bool {
		return bytes.Equal(data, []byte("value"))
	}

	mwAPI.On("Write", mock.MatchedBy(fn3)).Return(5, nil)

	mwAPI.On("Close").Return(nil)

	client := &Client{serviceAPI: msAPI}

	require.NoError(t, client.PutObject("bucket", "key", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 1)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "NewWriter", 1)

	mwAPI.AssertExpectations(t)
	mwAPI.AssertNumberOfCalls(t, "Write", 1)
	mwAPI.AssertNumberOfCalls(t, "SendMD5", 1)
	mwAPI.AssertNumberOfCalls(t, "SendCRC", 1)
	mwAPI.AssertNumberOfCalls(t, "Close", 1)
}

func TestClientAppendToObjectNotFoundOrEmpty(t *testing.T) {
	type test struct {
		name  string
		attrs *storage.ObjectAttrs
		err   error
	}

	tests := []*test{
		{
			name: "NotFound",
			err:  storage.ErrObjectNotExist,
		},
		{
			name:  "Empty",
			attrs: &storage.ObjectAttrs{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				msAPI = &mockServiceAPI{}
				mbAPI = &mockBucketAPI{}
				moAPI = &mockObjectAPI{}
				mwAPI = &mockWriterAPI{}
			)

			msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool {
				return bucket == "bucket"
			})).Return(mbAPI)

			mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return key == "key" })).Return(moAPI)

			moAPI.On("Attrs", mock.Anything).Return(test.attrs, test.err)
			moAPI.On("NewWriter", mock.Anything).Return(mwAPI, nil)

			mwAPI.On("SendMD5", mock.Anything)
			mwAPI.On("SendCRC", mock.Anything)

			fn1 := func(data []byte) bool {
				return bytes.Equal(data, []byte("value"))
			}

			mwAPI.On("Write", mock.MatchedBy(fn1)).Return(5, nil)

			mwAPI.On("Close").Return(nil)

			client := &Client{serviceAPI: msAPI}

			require.NoError(t, client.AppendToObject("bucket", "key", strings.NewReader("value")))

			msAPI.AssertExpectations(t)
			msAPI.AssertNumberOfCalls(t, "Bucket", 2)

			mbAPI.AssertExpectations(t)
			mbAPI.AssertNumberOfCalls(t, "Object", 2)

			moAPI.AssertExpectations(t)
			moAPI.AssertNumberOfCalls(t, "NewWriter", 1)

			mwAPI.AssertExpectations(t)
			mwAPI.AssertNumberOfCalls(t, "Write", 1)
			mwAPI.AssertNumberOfCalls(t, "SendMD5", 1)
			mwAPI.AssertNumberOfCalls(t, "SendCRC", 1)
			mwAPI.AssertNumberOfCalls(t, "Close", 1)
		})
	}
}

func TestClientAppendToObjectUsingObjectComposition(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
		mwAPI = &mockWriterAPI{}
		mcAPI = &mockComposeAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(
		func(key string) bool { return key == "key" || strings.HasPrefix(key, "key-") },
	)).Return(moAPI)

	moAPI.On("Attrs", mock.Anything).Return(&storage.ObjectAttrs{Size: 5}, nil)
	moAPI.On("NewWriter", mock.Anything).Return(mwAPI, nil)

	mwAPI.On("SendMD5", mock.Anything)
	mwAPI.On("SendCRC", mock.Anything)

	fn1 := func(data []byte) bool { return bytes.Equal(data, []byte("value")) }

	mwAPI.On("Write", mock.MatchedBy(fn1)).Return(5, nil)

	mwAPI.On("Close").Return(nil)

	fn2 := func(_ objectAPI) bool { return true }

	moAPI.On("ComposerFrom", mock.MatchedBy(fn2), mock.MatchedBy(fn2)).Return(mcAPI)

	mcAPI.On("Run", mock.Anything).Return(nil, nil)

	moAPI.On("Delete", mock.Anything).Return(nil)

	client := &Client{serviceAPI: msAPI}

	require.NoError(t, client.AppendToObject("bucket", "key", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 6)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 6)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "Attrs", 1)
	moAPI.AssertNumberOfCalls(t, "NewWriter", 1)
	moAPI.AssertNumberOfCalls(t, "ComposerFrom", 1)
	moAPI.AssertNumberOfCalls(t, "Delete", 1)

	mwAPI.AssertExpectations(t)
	mwAPI.AssertNumberOfCalls(t, "Write", 1)
	mwAPI.AssertNumberOfCalls(t, "SendMD5", 1)
	mwAPI.AssertNumberOfCalls(t, "SendCRC", 1)
	mwAPI.AssertNumberOfCalls(t, "Close", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "Run", 1)
}

func TestClientDeleteObjects(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return strings.HasPrefix(key, "key") })).Return(moAPI)

	moAPI.On("Delete", mock.Anything).Return(nil)

	client := &Client{serviceAPI: msAPI}

	require.NoError(t, client.DeleteObjects("bucket", "key1", "key2", "key3"))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 3)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 3)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "Delete", 3)
}

func TestClientDeleteDirectory(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		miAPI = &mockObjectIteratorAPI{}
		moAPI = &mockObjectAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.Anything).Return(moAPI)

	mbAPI.On("Objects", mock.Anything, mock.MatchedBy(
		func(query *storage.Query) bool { return query.Prefix == "prefix" },
	)).Return(miAPI)

	call := miAPI.On("Next").Return(&storage.ObjectAttrs{
		Name:    "/path/to/key1",
		Size:    64,
		Updated: (time.Time{}).Add(24 * time.Hour),
	}, nil)

	call.Repeatability = 1

	call = miAPI.On("Next").Return(&storage.ObjectAttrs{
		Name:    "/path/to/key2",
		Size:    128,
		Updated: (time.Time{}).Add(48 * time.Hour),
	}, nil)

	call.Repeatability = 1

	miAPI.On("Next").Return(nil, iterator.Done)

	moAPI.On("Delete", mock.Anything).Return(nil)

	client := &Client{serviceAPI: msAPI}

	require.NoError(t, client.DeleteDirectory("bucket", "prefix"))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 3)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 2)
	mbAPI.AssertNumberOfCalls(t, "Objects", 1)

	miAPI.AssertExpectations(t)
	miAPI.AssertNumberOfCalls(t, "Next", 3)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "Delete", 2)
}

func TestClientIterateObjects(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		miAPI = &mockObjectIteratorAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Objects", mock.Anything, mock.MatchedBy(
		func(query *storage.Query) bool { return query.Prefix == "prefix" },
	)).Return(miAPI)

	miAPI.On("Next").Return(nil, iterator.Done)

	client := &Client{serviceAPI: msAPI}

	require.NoError(t, client.IterateObjects("bucket", "prefix", nil, nil, nil))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Objects", 1)

	miAPI.AssertExpectations(t)
	miAPI.AssertNumberOfCalls(t, "Next", 1)
}

func TestClientIterateObjectsBothIncludeExcludeSupplied(t *testing.T) {
	client := &Client{}

	err := client.IterateObjects("bucket", "prefix", []*regexp.Regexp{}, []*regexp.Regexp{}, nil)
	require.ErrorIs(t, err, objcli.ErrIncludeAndExcludeAreMutuallyExclusive)
}

func TestClientIterateObjectsPropagateUserError(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		miAPI = &mockObjectIteratorAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Objects", mock.Anything, mock.MatchedBy(
		func(query *storage.Query) bool { return query.Prefix == "prefix" },
	)).Return(miAPI)

	miAPI.On("Next").Return(&storage.ObjectAttrs{}, nil)

	client := &Client{serviceAPI: msAPI}

	err := client.IterateObjects("bucket", "prefix", nil, nil, func(attrs *objval.ObjectAttrs) error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Objects", 1)

	miAPI.AssertExpectations(t)
	miAPI.AssertNumberOfCalls(t, "Next", 1)
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
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         128,
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
				},
				{
					Key:          "/path/to/key2",
					Size:         256,
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
					Size:         64,
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
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         128,
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
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/key1",
					Size:         128,
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
					Size:         256,
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
					Size:         256,
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
					Size:         256,
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				msAPI = &mockServiceAPI{}
				mbAPI = &mockBucketAPI{}
				miAPI = &mockObjectIteratorAPI{}
			)

			msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

			mbAPI.On("Objects", mock.Anything, mock.Anything).Return(miAPI)

			call := miAPI.On("Next").Return(&storage.ObjectAttrs{
				Name:    "/path/to/key1",
				Size:    64,
				Updated: (time.Time{}).Add(24 * time.Hour),
			}, nil)

			call.Repeatability = 1

			call = miAPI.On("Next").Return(&storage.ObjectAttrs{
				Name:    "/path/to/another/key1",
				Size:    128,
				Updated: (time.Time{}).Add(48 * time.Hour),
			}, nil)

			call.Repeatability = 1

			call = miAPI.On("Next").Return(&storage.ObjectAttrs{
				Name:    "/path/to/key2",
				Size:    256,
				Updated: (time.Time{}).Add(72 * time.Hour),
			}, nil)

			call.Repeatability = 1

			miAPI.On("Next").Return(nil, iterator.Done)

			client := &Client{serviceAPI: msAPI}

			var all []*objval.ObjectAttrs

			err := client.IterateObjects("bucket", "", test.include, test.exclude, func(attrs *objval.ObjectAttrs) error {
				all = append(all, attrs)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, test.all, all)

			msAPI.AssertExpectations(t)
			msAPI.AssertNumberOfCalls(t, "Bucket", 1)

			mbAPI.AssertExpectations(t)
			mbAPI.AssertNumberOfCalls(t, "Objects", 1)

			miAPI.AssertExpectations(t)
			miAPI.AssertNumberOfCalls(t, "Next", 4)
		})
	}
}

func TestClientCreateMultipartUpload(t *testing.T) {
	client := &Client{}

	id, err := client.CreateMultipartUpload("bucket", "key")
	require.NoError(t, err)
	require.NotEmpty(t, id)
}

func TestClientListParts(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		miAPI = &mockObjectIteratorAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Objects", mock.Anything, mock.MatchedBy(
		func(query *storage.Query) bool { return query.Prefix == "key-mpu-id" },
	)).Return(miAPI)

	call := miAPI.On("Next").Return(&storage.ObjectAttrs{
		Name:    "key-mpu-id-uuid1",
		Size:    64,
		Updated: (time.Time{}).Add(24 * time.Hour),
	}, nil)

	call.Repeatability = 1

	call = miAPI.On("Next").Return(&storage.ObjectAttrs{
		Name:    "key-mpu-id-uuid2",
		Size:    128,
		Updated: (time.Time{}).Add(48 * time.Hour),
	}, nil)

	call.Repeatability = 1

	miAPI.On("Next").Return(nil, iterator.Done)

	client := &Client{serviceAPI: msAPI}

	parts, err := client.ListParts("bucket", "id", "key")
	require.NoError(t, err)

	expected := []objval.Part{{ID: "key-mpu-id-uuid1", Size: 64}, {ID: "key-mpu-id-uuid2", Size: 128}}
	require.Equal(t, expected, parts)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Objects", 1)
}

func TestClientUploadPart(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
		mwAPI = &mockWriterAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(func(key string) bool { return strings.HasPrefix(key, "key-") })).Return(moAPI)

	moAPI.On("NewWriter", mock.Anything).Return(mwAPI, nil)

	fn1 := func(sum []byte) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(sum, expected[:])
	}

	mwAPI.On("SendMD5", mock.MatchedBy(fn1))

	fn2 := func(sum uint32) bool {
		hasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		hasher.Write([]byte("value"))

		return sum == hasher.Sum32()
	}

	mwAPI.On("SendCRC", mock.MatchedBy(fn2))

	fn3 := func(data []byte) bool {
		return bytes.Equal(data, []byte("value"))
	}

	mwAPI.On("Write", mock.MatchedBy(fn3)).Return(5, nil)

	mwAPI.On("Close").Return(nil)

	client := &Client{serviceAPI: msAPI}

	part, err := client.UploadPart("bucket", "id", "key", 42, strings.NewReader("value"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(part.ID, "key-"))
	require.Equal(t, 42, part.Number)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 1)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "NewWriter", 1)

	mwAPI.AssertExpectations(t)
	mwAPI.AssertNumberOfCalls(t, "Write", 1)
	mwAPI.AssertNumberOfCalls(t, "SendMD5", 1)
	mwAPI.AssertNumberOfCalls(t, "SendCRC", 1)
	mwAPI.AssertNumberOfCalls(t, "Close", 1)
}

func TestClientCompleteMultipartUploadOverMaxComposable(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		moAPI = &mockObjectAPI{}
		mcAPI = &mockComposeAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Object", mock.MatchedBy(
		func(key string) bool { return key == "key" || strings.HasPrefix(key, "key-") },
	)).Return(moAPI)

	expected := make([]interface{}, 0, MaxComposable)

	for i := 0; i < MaxComposable; i++ {
		expected = append(expected, mock.Anything)
	}

	moAPI.On("ComposerFrom", expected...).Return(mcAPI)

	mcAPI.On("Run", mock.Anything).Return(nil, nil)

	moAPI.On("Delete", mock.Anything).Return(nil)

	client := &Client{serviceAPI: msAPI}

	parts := make([]objval.Part, 0)

	for i := 1; i < MaxComposable*2+42; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("key-%d", i), Number: i})
	}

	require.NoError(t, client.CompleteMultipartUpload("bucket", "id", "key", parts...))

	msAPI.AssertExpectations(t)
	mbAPI.AssertExpectations(t)
	moAPI.AssertExpectations(t)
	mcAPI.AssertExpectations(t)
}

func TestClientAbortMultipartUpload(t *testing.T) {
	var (
		msAPI = &mockServiceAPI{}
		mbAPI = &mockBucketAPI{}
		miAPI = &mockObjectIteratorAPI{}
		moAPI = &mockObjectAPI{}
	)

	msAPI.On("Bucket", mock.MatchedBy(func(bucket string) bool { return bucket == "bucket" })).Return(mbAPI)

	mbAPI.On("Objects", mock.Anything, mock.MatchedBy(
		func(query *storage.Query) bool { return query.Prefix == "key-mpu-id" },
	)).Return(miAPI)

	call := miAPI.On("Next").Return(&storage.ObjectAttrs{
		Name:    "/path/to/key-mpu-id-f2be662e-458f-4e26-b2d7-74e7cf78edc7",
		Size:    64,
		Updated: (time.Time{}).Add(24 * time.Hour),
	}, nil)

	call.Repeatability = 1

	miAPI.On("Next").Return(nil, iterator.Done)

	mbAPI.On("Object", mock.Anything).Return(moAPI)

	moAPI.On("Delete", mock.Anything).Return(nil)

	client := &Client{serviceAPI: msAPI}

	require.NoError(t, client.AbortMultipartUpload("bucket", "id", "key"))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "Bucket", 2)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Object", 1)

	moAPI.AssertExpectations(t)
	moAPI.AssertNumberOfCalls(t, "Delete", 1)
}
