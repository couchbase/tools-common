package objazure

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objval"
)

// accessUnexportedField the Azure SDK returns structs which encapsulate a '*http.Response', to be able to mock the API
// we need to be able to access fields from these unexported structs.
func accessUnexportedField(value reflect.Value, field string) reflect.Value {
	return value.Elem().FieldByName(field)
}

// assignToUnexportedField the Azure SDK returns structs which encapsulate a '*http.Response'; these structs are all
// unexported (but expose public functions). To be able to mock the API we must be able to assign to these unexported
// fields.
func assignToUnexportedField(field reflect.Value, value any) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

// storageError returns an azure SDK error where the response status is set to the given status.
func storageError(status int) *azblob.StorageError {
	var (
		err  = &azblob.StorageError{}
		resp = accessUnexportedField(reflect.ValueOf(err), "response")
	)

	assignToUnexportedField(resp, &http.Response{StatusCode: status})

	return err
}

func TestNewClient(t *testing.T) {
	require.Equal(t, &Client{storageAPI: serviceClient{client: nil}}, NewClient(nil))
}

func TestClientProvider(t *testing.T) {
	require.Equal(t, objval.ProviderAzure, (&Client{}).Provider())
}

func TestClientGetObject(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	output := azblob.BlobDownloadResponse{}

	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = aws.Int64(42)
	output.RawResponse = &http.Response{
		Body: io.NopCloser(strings.NewReader("value")),
	}

	mbAPI.On(
		"Download",
		mock.Anything,
		mock.MatchedBy(func(o azblob.BlobDownloadOptions) bool { return *o.Offset == 0 && *o.Count == 0 }),
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	object, err := client.GetObject("container", "blob", nil)
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         42,
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Download", 1)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	output := azblob.BlobDownloadResponse{}

	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = aws.Int64(42)
	output.RawResponse = &http.Response{
		Body: io.NopCloser(strings.NewReader("value")),
	}

	mbAPI.On(
		"Download",
		mock.Anything,
		mock.MatchedBy(func(o azblob.BlobDownloadOptions) bool { return *o.Offset == 64 && *o.Count == 65 }),
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	object, err := client.GetObject("container", "blob", &objval.ByteRange{Start: 64, End: 128})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         42,
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Download", 1)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject("bucket", "blob", &objval.ByteRange{Start: 128, End: 64})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientGetObjectAttrs(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	output := azblob.BlobGetPropertiesResponse{}

	output.ContentLength = aws.Int64(42)
	output.ETag = aws.String("etag")
	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))

	mbAPI.On("GetProperties", mock.Anything, mock.Anything).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	attrs, err := client.GetObjectAttrs("container", "blob")
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "blob",
		ETag:         "etag",
		Size:         42,
		LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	require.Equal(t, expected, attrs)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "GetProperties", 1)
}

func TestClientPutObject(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	output := azblob.BlockBlobUploadResponse{}

	fn1 := func(options azblob.BlockBlobUploadOptions) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(options.TransactionalContentMD5, expected[:])
	}

	mbAPI.On(
		"Upload",
		mock.Anything,
		strings.NewReader("value"),
		mock.MatchedBy(fn1),
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.PutObject("container", "blob", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Upload", 1)
}

func TestClientAppendToObjectNotExists(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	mbAPI.On("GetBlockList", mock.Anything, mock.Anything, mock.Anything).Return(
		azblob.BlockBlobGetBlockListResponse{},
		&azblob.StorageError{ErrorCode: azblob.StorageErrorCodeBlobNotFound},
	)

	fn2 := func(options azblob.BlockBlobStageBlockOptions) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(options.TransactionalContentMD5, expected[:])
	}

	mbAPI.On(
		"StageBlock",
		mock.Anything,
		mock.Anything,
		strings.NewReader("value"),
		mock.MatchedBy(fn2),
	).Return(azblob.BlockBlobStageBlockResponse{}, nil)

	mbAPI.On(
		"CommitBlockList",
		mock.Anything,
		mock.MatchedBy(func(parts []string) bool { return len(parts) == 1 }),
		mock.Anything,
	).Return(azblob.BlockBlobCommitBlockListResponse{}, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.AppendToObject("container", "blob", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 3)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "StageBlock", 1)
	mbAPI.AssertNumberOfCalls(t, "CommitBlockList", 1)
}

func TestClientAppendToObject(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	listOutput := azblob.BlockBlobGetBlockListResponse{}
	listOutput.BlockList = azblob.BlockList{
		CommittedBlocks: []*azblob.Block{
			{
				Name: aws.String("block1"),
				Size: aws.Int64(64),
			},
		},
	}

	mbAPI.On("GetBlockList", mock.Anything, mock.Anything, mock.Anything).Return(listOutput, nil)

	fn2 := func(options azblob.BlockBlobStageBlockOptions) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(options.TransactionalContentMD5, expected[:])
	}

	mbAPI.On(
		"StageBlock",
		mock.Anything,
		mock.Anything,
		strings.NewReader("value"),
		mock.MatchedBy(fn2),
	).Return(azblob.BlockBlobStageBlockResponse{}, nil)

	mbAPI.On(
		"CommitBlockList",
		mock.Anything,
		mock.MatchedBy(func(parts []string) bool { return len(parts) == 2 }),
		mock.Anything,
	).Return(azblob.BlockBlobCommitBlockListResponse{}, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.AppendToObject("container", "blob", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 3)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "GetBlockList", 1)
	mbAPI.AssertNumberOfCalls(t, "StageBlock", 1)
	mbAPI.AssertNumberOfCalls(t, "CommitBlockList", 1)
}

func TestClientDeleteObjects(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob1" })).Return(mbAPI, nil)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob2" })).Return(mbAPI, nil)

	mbAPI.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(azblob.BlobDeleteResponse{}, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.DeleteObjects("container", "blob1", "blob2"))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 2)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Delete", 2)
}

func TestClientDeleteObjectsKeyNotFound(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mbAPI, nil)

	mbAPI.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(
		azblob.BlobDeleteResponse{},
		&azblob.StorageError{ErrorCode: azblob.StorageErrorCodeBlobNotFound},
	)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.DeleteObjects("container", "blob"))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Delete", 1)
}

func TestClientDeleteDirectory(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
		mpAPI = &mockListBlobsPagerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	blobs := []*azblob.BlobItemInternal{
		{
			Name: aws.String("blob1"),
			Properties: &azblob.BlobPropertiesInternal{
				ContentLength: aws.Int64(64),
				LastModified:  aws.Time((time.Time{}).Add(48 * time.Hour)),
			},
		},
		{
			Name: aws.String("blob2"),
			Properties: &azblob.BlobPropertiesInternal{
				ContentLength: aws.Int64(128),
				LastModified:  aws.Time((time.Time{}).Add(48 * time.Hour)),
			},
		},
	}

	mcAPI.On(
		"GetListBlobsFlatPagerAPI",
		mock.Anything,
	).Return(mpAPI)

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, blobs, nil).Once()

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, nil, errPagerNoMorePages).Once()

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob1" })).Return(mbAPI, nil)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob2" })).Return(mbAPI, nil)

	mbAPI.On("Delete", mock.Anything, mock.Anything).Return(azblob.BlobDeleteResponse{}, nil)

	client := &Client{storageAPI: msAPI}

	err := client.DeleteDirectory("container", "")
	require.NoError(t, err)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 3)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "GetListBlobsFlatPagerAPI", 1)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 2)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Delete", 2)

	mpAPI.AssertExpectations(t)
	mpAPI.AssertNumberOfCalls(t, "GetNextListBlobsSegment", 2)
}

func TestClientIterateObjects(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mpAPI = &mockListBlobsPagerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	blobs := make([]*azblob.BlobItemInternal, 0)

	fn1 := func(options azblob.ContainerListBlobsFlatOptions) bool {
		return options.Marker == nil && *options.Prefix == "prefix"
	}

	mcAPI.On(
		"GetListBlobsFlatPagerAPI",
		mock.MatchedBy(fn1),
	).Return(mpAPI)

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, blobs, nil).Once()

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, nil, errPagerNoMorePages).Once()

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.IterateObjects("container", "prefix", "", nil, nil, nil))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "GetListBlobsFlatPagerAPI", 1)

	mpAPI.AssertExpectations(t)
	mpAPI.AssertNumberOfCalls(t, "GetNextListBlobsSegment", 2)
}

func TestClientIterateObjectsWithoutDelimiter(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mpAPI = &mockListBlobsPagerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	blobs := []*azblob.BlobItemInternal{
		{
			Name: aws.String("blob"),
			Properties: &azblob.BlobPropertiesInternal{
				ContentLength: aws.Int64(42),
				LastModified:  aws.Time((time.Time{}).Add(24 * time.Hour)),
			},
		},
	}

	fn1 := func(options azblob.ContainerListBlobsFlatOptions) bool {
		return options.Marker == nil && *options.Prefix == "prefix"
	}

	mcAPI.On(
		"GetListBlobsFlatPagerAPI",
		mock.MatchedBy(fn1),
	).Return(mpAPI)

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, blobs, nil).Once()

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, nil, errPagerNoMorePages).Once()

	var (
		client  = &Client{storageAPI: msAPI}
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

	require.NoError(t, client.IterateObjects("container", "prefix", "", nil, nil, fn))
	require.Zero(t, dirs)
	require.Equal(t, 1, objects)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "GetListBlobsFlatPagerAPI", 1)

	mpAPI.AssertExpectations(t)
	mpAPI.AssertNumberOfCalls(t, "GetNextListBlobsSegment", 2)
}

func TestClientIterateObjectsWithDelimiter(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mpAPI = &mockListBlobsPagerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	prefixes := []*azblob.BlobPrefix{
		{
			Name: aws.String("prefix"),
		},
	}

	blobs := []*azblob.BlobItemInternal{
		{
			Name: aws.String("blob"),
			Properties: &azblob.BlobPropertiesInternal{
				ContentLength: aws.Int64(42),
				LastModified:  aws.Time(time.Time{}.Add(24 * time.Hour)),
			},
		},
	}

	fn1 := func(options azblob.ContainerListBlobsHierarchyOptions) bool {
		return options.Marker == nil && *options.Prefix == "prefix"
	}

	mcAPI.On(
		"GetListBlobsHierarchyPagerAPI",
		mock.MatchedBy(func(delimiter string) bool { return delimiter == "delimiter" }),
		mock.MatchedBy(fn1),
	).Return(mpAPI)

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(prefixes, blobs, nil).Once()

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, nil, errPagerNoMorePages).Once()

	var (
		client  = &Client{storageAPI: msAPI}
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

	require.NoError(t, client.IterateObjects("container", "prefix", "delimiter", nil, nil, fn))
	require.Equal(t, 1, dirs)
	require.Equal(t, 1, objects)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "GetListBlobsHierarchyPagerAPI", 1)

	mpAPI.AssertExpectations(t)
	mpAPI.AssertNumberOfCalls(t, "GetNextListBlobsSegment", 2)
}

func TestClientIterateObjectsBothIncludeExcludeSupplied(t *testing.T) {
	client := &Client{}

	err := client.IterateObjects("bucket", "prefix", "delimiter", []*regexp.Regexp{}, []*regexp.Regexp{}, nil)
	require.ErrorIs(t, err, objcli.ErrIncludeAndExcludeAreMutuallyExclusive)
}

func TestClientIterateObjectsPropagateUserError(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mpAPI = &mockListBlobsPagerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI, nil)

	blobs := []*azblob.BlobItemInternal{
		{
			Name: aws.String("blob"),
			Properties: &azblob.BlobPropertiesInternal{
				ContentLength: aws.Int64(42),
				LastModified:  aws.Time(time.Time{}.Add(24 * time.Hour)),
			},
		},
	}

	fn1 := func(options azblob.ContainerListBlobsHierarchyOptions) bool {
		return options.Marker == nil && *options.Prefix == "prefix"
	}

	mcAPI.On(
		"GetListBlobsHierarchyPagerAPI",
		mock.MatchedBy(func(delimiter string) bool { return delimiter == "delimiter" }),
		mock.MatchedBy(fn1),
	).Return(mpAPI)

	mpAPI.On(
		"GetNextListBlobsSegment",
		mock.Anything,
	).Return(nil, blobs, nil).Once()

	client := &Client{storageAPI: msAPI}

	err := client.IterateObjects("container", "prefix", "delimiter", nil, nil, func(attrs *objval.ObjectAttrs) error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "GetListBlobsHierarchyPagerAPI", 1)

	mpAPI.AssertExpectations(t)
	mpAPI.AssertNumberOfCalls(t, "GetNextListBlobsSegment", 1)
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
					Key:          "/path/to/blob1",
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/blob1",
					Size:         128,
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
				},
				{
					Key:          "/path/to/blob2",
					Size:         256,
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
		{
			name:    "IncludeWithFullPath",
			include: []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("/path/to/blob1"))},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/blob1",
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
			},
		},
		{
			name:    "IncludeWithBasename",
			include: []*regexp.Regexp{regexp.MustCompile("blob1")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/blob1",
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/blob1",
					Size:         128,
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
				},
			},
		},
		{
			name:    "IncludeMustMatchBasename",
			include: []*regexp.Regexp{regexp.MustCompile("^blob1$")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/blob1",
					Size:         64,
					LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
				},
				{
					Key:          "/path/to/another/blob1",
					Size:         128,
					LastModified: aws.Time((time.Time{}).Add(48 * time.Hour)),
				},
			},
		},
		{
			name:    "ExcludeWithFullPath",
			exclude: []*regexp.Regexp{regexp.MustCompile("blob1")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/blob2",
					Size:         256,
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
		{
			name:    "ExcludeWithBasename",
			exclude: []*regexp.Regexp{regexp.MustCompile("blob1")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/blob2",
					Size:         256,
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
		{
			name:    "ExcludeMustMatchBasename",
			exclude: []*regexp.Regexp{regexp.MustCompile("^blob1$")},
			all: []*objval.ObjectAttrs{
				{
					Key:          "/path/to/blob2",
					Size:         256,
					LastModified: aws.Time((time.Time{}).Add(72 * time.Hour)),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				msAPI = &mockBlobStorageAPI{}
				mcAPI = &mockContainerAPI{}
				mpAPI = &mockListBlobsPagerAPI{}
			)

			msAPI.On("ToContainerAPI", mock.MatchedBy(
				func(container string) bool { return container == "container" })).Return(mcAPI, nil)

			blobs := []string{
				"/path/to/blob1",
				"/path/to/another/blob1",
				"/path/to/blob2",
				"", // Final call should return an empty name (marker) which indicates the end of iteration
			}

			output := make([]*azblob.BlobItemInternal, 0, 3)

			for i := int64(1); i <= 3; i++ {
				blobTime := (time.Time{}).Add(time.Duration(i*24) * time.Hour)
				blob := &azblob.BlobItemInternal{
					Name: &blobs[i-1],
					Properties: &azblob.BlobPropertiesInternal{
						ContentLength: aws.Int64(64 << (i - 1)),
						LastModified:  &blobTime,
					},
				}

				output = append(output, blob)
			}

			mcAPI.On(
				"GetListBlobsFlatPagerAPI",
				mock.Anything,
			).Return(mpAPI)

			mpAPI.On(
				"GetNextListBlobsSegment",
				mock.Anything,
			).Return(nil, output, nil).Once()

			mpAPI.On(
				"GetNextListBlobsSegment",
				mock.Anything,
			).Return(nil, nil, errPagerNoMorePages).Once()

			client := &Client{storageAPI: msAPI}

			var all []*objval.ObjectAttrs

			err := client.IterateObjects("container", "", "", test.include, test.exclude, func(attrs *objval.ObjectAttrs) error {
				all = append(all, attrs)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, test.all, all)

			msAPI.AssertExpectations(t)
			msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

			mcAPI.AssertExpectations(t)
			mcAPI.AssertNumberOfCalls(t, "GetListBlobsFlatPagerAPI", 1)

			mpAPI.AssertExpectations(t)
			mpAPI.AssertNumberOfCalls(t, "GetNextListBlobsSegment", 2)
		})
	}
}

func TestClientCreateMultipartUpload(t *testing.T) {
	client := &Client{}

	id, err := client.CreateMultipartUpload("container", "blob")
	require.NoError(t, err)
	require.Equal(t, objcli.NoUploadID, id)
}

func TestClientUploadPartWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPart("container", "id", "blob", 42, nil)
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientListParts(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	output := azblob.BlockBlobGetBlockListResponse{}
	output.BlockList = azblob.BlockList{
		UncommittedBlocks: []*azblob.Block{
			{
				Name: aws.String("block3"),
				Size: aws.Int64(256),
			},
			{
				Name: aws.String("block4"),
				Size: aws.Int64(512),
			},
		},
	}

	mbAPI.On("GetBlockList", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	parts, err := client.ListParts("container", objcli.NoUploadID, "blob")
	require.NoError(t, err)

	expected := []objval.Part{{ID: "block3", Size: 256}, {ID: "block4", Size: 512}}
	require.Equal(t, expected, parts)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "GetBlockList", 1)
}

func TestClientListPartsWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.ListParts("container", "id", "blob")
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientUploadPart(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	fn1 := func(options azblob.BlockBlobStageBlockOptions) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(options.TransactionalContentMD5, expected[:])
	}

	mbAPI.On(
		"StageBlock",
		mock.Anything,
		mock.Anything,
		strings.NewReader("value"),
		mock.MatchedBy(fn1),
	).Return(azblob.BlockBlobStageBlockResponse{}, nil)

	client := &Client{storageAPI: msAPI}

	part, err := client.UploadPart("container", objcli.NoUploadID, "blob", 42, strings.NewReader("value"))
	require.NoError(t, err)
	require.NotZero(t, part.ID)

	_, err = base64.StdEncoding.DecodeString(part.ID)
	require.NoError(t, err)

	expected := objval.Part{
		ID:     part.ID,
		Number: 42,
		Size:   5,
	}

	require.Equal(t, expected, part)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "StageBlock", 1)
}

func TestClientUploadPartCopy(t *testing.T) {
	type test struct {
		name    string
		br      *objval.ByteRange
		eOffset int64
		eLength int64
	}

	tests := []*test{
		{
			name:    "NoByteRange",
			eLength: azblob.CountToEnd,
		},
		{
			name:    "WithByteRange",
			br:      &objval.ByteRange{Start: 64, End: 128},
			eOffset: 64,
			eLength: 65,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				msAPI       = &mockBlobStorageAPI{}
				mSrcBlobAPI = &mockBlobAPI{}
				mDstBlobAPI = &mockBlobAPI{}
			)

			msAPI.On(
				"ToBlobAPI",
				mock.MatchedBy(func(container string) bool { return container == "container" }),
				mock.MatchedBy(func(blob string) bool { return blob == "dst" }),
			).Return(mDstBlobAPI, nil)

			msAPI.On(
				"ToBlobAPI",
				mock.MatchedBy(func(container string) bool { return container == "container" }),
				mock.MatchedBy(func(blob string) bool { return blob == "src" }),
			).Return(mSrcBlobAPI, nil)

			mSrcBlobAPI.On("URL").Return("example.com")

			fn1 := func(blob string) bool {
				_, err := base64.StdEncoding.DecodeString(blob)
				return err == nil
			}

			fn2 := func(options azblob.BlockBlobStageBlockFromURLOptions) bool {
				return *options.Offset == test.eOffset && *options.Count == test.eLength
			}

			mDstBlobAPI.On(
				"StageBlockFromURL",
				mock.Anything,
				mock.MatchedBy(fn1),
				mock.MatchedBy(func(s string) bool { return "example.com" == s }),
				mock.MatchedBy(func(length int64) bool { return length == 0 }),
				mock.MatchedBy(fn2),
			).Return(azblob.BlockBlobStageBlockFromURLResponse{}, nil)

			client := &Client{storageAPI: msAPI}

			part, err := client.UploadPartCopy("container", objcli.NoUploadID, "dst", "src", 42, test.br)
			require.NoError(t, err)
			require.NotZero(t, part.ID)

			_, err = base64.StdEncoding.DecodeString(part.ID)
			require.NoError(t, err)

			msAPI.AssertExpectations(t)
			msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 2)

			mDstBlobAPI.AssertExpectations(t)
			mDstBlobAPI.AssertNumberOfCalls(t, "StageBlockFromURL", 1)

			mSrcBlobAPI.AssertExpectations(t)
			mSrcBlobAPI.AssertNumberOfCalls(t, "URL", 1)
		})
	}
}

func TestClientUploadPartCopyWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy(
		"bucket",
		objcli.NoUploadID,
		"dst",
		"src",
		42,
		&objval.ByteRange{Start: 128, End: 64},
	)

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientUploadPartCopyWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy("bucket", "id", "dst", "src", 42, nil)
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.CompleteMultipartUpload("bucket", "id", "blob", objval.Part{})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadOverMaxComposable(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On(
		"ToBlobAPI",
		mock.MatchedBy(func(container string) bool { return container == "container" }),
		mock.MatchedBy(func(blob string) bool { return blob == "blob" }),
	).Return(mbAPI, nil)

	fn1 := func(parts []string) bool {
		return slices.Equal(parts, []string{"blob1", "blob2", "blob3"})
	}

	mbAPI.On(
		"CommitBlockList",
		mock.Anything,
		mock.MatchedBy(fn1),
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(azblob.BlockBlobCommitBlockListResponse{}, nil)

	client := &Client{storageAPI: msAPI}

	parts := make([]objval.Part, 0)

	for i := 1; i <= 3; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("blob%d", i), Number: i})
	}

	require.NoError(t, client.CompleteMultipartUpload("container", objcli.NoUploadID, "blob", parts...))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "CommitBlockList", 1)
}

func TestClientAbortMultipartUpload(t *testing.T) {
	client := &Client{}

	require.NoError(t, client.AbortMultipartUpload("container", objcli.NoUploadID, "blob"))
}

func TestClientAbortMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.AbortMultipartUpload("container", "id", "blob")
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}
