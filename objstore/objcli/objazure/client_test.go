package objazure

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/slice"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// responseField is the unexported field used by the Azure SDK to store the HTTP response. We access/set/modify this
// field for unit testing purposes.
const responseField = "rawResponse"

// accessUnexportedField the Azure SDK returns structs which encapsulate a '*http.Response', to be able to mock the API
// we need to be able to access fields from these unexported structs.
func accessUnexportedField(value reflect.Value, field string) reflect.Value {
	return value.Elem().FieldByName(field)
}

// allocateUnexportedField the Azure SDK may return a struct which has a pointer field to an unexported struct, we need
// to be able to allocate this field so that we can assign to the encapsulated '*http.Response'.
func allocateUnexportedField(field reflect.Value) reflect.Value {
	return reflect.New(field.Type().Elem())
}

// assignToUnexportedField the Azure SDK returns structs which encapsulate a '*http.Response'; these structs are all
// unexported (but expose public functions). To be able to mock the API we must be able to assign to these unexported
// fields.
func assignToUnexportedField(field reflect.Value, value interface{}) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

type mockError struct {
	resp  *http.Response
	inner azblob.ServiceCodeType
}

func (e *mockError) Error() string                       { return "" }
func (e *mockError) Timeout() bool                       { return false }
func (e *mockError) Temporary() bool                     { return false }
func (e *mockError) Response() *http.Response            { return e.resp }
func (e *mockError) ServiceCode() azblob.ServiceCodeType { return e.inner }

func TestNewClient(t *testing.T) {
	require.Equal(t, &Client{storageAPI: serviceURL{url: azblob.ServiceURL{}}}, NewClient(azblob.ServiceURL{}))
}

func TestClientGetObject(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mbAPI)

	output := &azblob.DownloadResponse{}
	r := accessUnexportedField(reflect.ValueOf(output), "r")

	allocated := allocateUnexportedField(r)
	rawResponse := accessUnexportedField(allocated, responseField)

	assignToUnexportedField(rawResponse, &http.Response{
		Header: http.Header{
			"Content-Length": {"42"},
			"Last-Modified":  {(time.Time{}).Add(24 * time.Hour).Format(time.RFC1123)},
		},
		Body: io.NopCloser(strings.NewReader("value")),
	})

	assignToUnexportedField(r, allocated.Interface())

	mbAPI.On(
		"Download",
		mock.Anything,
		mock.MatchedBy(func(offset int64) bool { return offset == 0 }),
		mock.MatchedBy(func(length int64) bool { return length == 0 }),
		mock.Anything,
		mock.Anything,
		mock.Anything,
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
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Download", 1)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mbAPI)

	output := &azblob.DownloadResponse{}
	r := accessUnexportedField(reflect.ValueOf(output), "r")

	allocated := allocateUnexportedField(r)
	rawResponse := accessUnexportedField(allocated, responseField)

	assignToUnexportedField(rawResponse, &http.Response{
		Header: http.Header{
			"Content-Length": {"42"},
			"Last-Modified":  {(time.Time{}).Add(24 * time.Hour).Format(time.RFC1123)},
		},
		Body: io.NopCloser(strings.NewReader("value")),
	})

	assignToUnexportedField(r, allocated.Interface())

	mbAPI.On(
		"Download",
		mock.Anything,
		mock.MatchedBy(func(offset int64) bool { return offset == 64 }),
		mock.MatchedBy(func(length int64) bool { return length == 65 }),
		mock.Anything,
		mock.Anything,
		mock.Anything,
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
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Download", 1)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject("bucket", "key", &objval.ByteRange{Start: 128, End: 64})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientGetObjectAttrs(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mbAPI)

	output := &azblob.BlobGetPropertiesResponse{}
	rawResponse := accessUnexportedField(reflect.ValueOf(output), responseField)

	assignToUnexportedField(rawResponse, &http.Response{
		Header: http.Header{
			"Content-Length": {"42"},
			"Etag":           {"etag"},
			"Last-Modified":  {(time.Time{}).Add(24 * time.Hour).Format(time.RFC1123)},
		},
	})

	mbAPI.On("GetProperties", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

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
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "GetProperties", 1)
}

func TestClientPutObject(t *testing.T) {
	var (
		msAPI     = &mockBlobStorageAPI{}
		mcAPI     = &mockContainerAPI{}
		mBlobAPI  = &mockBlobAPI{}
		mBlockAPI = &mockBlockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mBlobAPI)

	mBlobAPI.On("ToBlockBlobAPI").Return(mBlockAPI)

	fn1 := func(headers azblob.BlobHTTPHeaders) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(headers.ContentMD5, expected[:])
	}

	output := &azblob.BlockBlobUploadResponse{}
	rawResponse := accessUnexportedField(reflect.ValueOf(output), responseField)

	assignToUnexportedField(rawResponse, &http.Response{})

	mBlockAPI.On(
		"Upload",
		mock.Anything,
		strings.NewReader("value"),
		mock.MatchedBy(fn1),
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.PutObject("container", "blob", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mBlobAPI.AssertExpectations(t)
	mBlobAPI.AssertNumberOfCalls(t, "ToBlockBlobAPI", 1)

	mBlockAPI.AssertExpectations(t)
	mBlockAPI.AssertNumberOfCalls(t, "Upload", 1)
}

func TestClientAppendToObjectNotExists(t *testing.T) {
	var (
		msAPI     = &mockBlobStorageAPI{}
		mcAPI     = &mockContainerAPI{}
		mBlobAPI  = &mockBlobAPI{}
		mBlockAPI = &mockBlockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mBlobAPI)

	mBlobAPI.On("ToBlockBlobAPI").Return(mBlockAPI)

	mBlobAPI.On("GetProperties", mock.Anything, mock.Anything, mock.Anything).Return(
		nil,
		&mockError{inner: azblob.ServiceCodeBlobNotFound},
	)

	fn1 := func(headers azblob.BlobHTTPHeaders) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(headers.ContentMD5, expected[:])
	}

	output := &azblob.BlockBlobUploadResponse{}
	rawResponse := accessUnexportedField(reflect.ValueOf(output), responseField)

	assignToUnexportedField(rawResponse, &http.Response{})

	mBlockAPI.On(
		"Upload",
		mock.Anything,
		strings.NewReader("value"),
		mock.MatchedBy(fn1),
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.AppendToObject("container", "blob", strings.NewReader("value")))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 2)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 2)

	mBlobAPI.AssertExpectations(t)
	mBlobAPI.AssertNumberOfCalls(t, "GetProperties", 1)
	mBlobAPI.AssertNumberOfCalls(t, "ToBlockBlobAPI", 1)

	mBlockAPI.AssertExpectations(t)
	mBlockAPI.AssertNumberOfCalls(t, "Upload", 1)
}

func TestClientAppendToObject(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}

		mBlobAPI  = &mockBlobAPI{}
		mBlockAPI = &mockBlockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.Anything).Return(mBlobAPI)

	mBlobAPI.On("ToBlockBlobAPI").Return(mBlockAPI)

	output := &azblob.BlobGetPropertiesResponse{}
	rawResponse := accessUnexportedField(reflect.ValueOf(output), responseField)

	assignToUnexportedField(rawResponse, &http.Response{
		Header: http.Header{
			"Content-Length": {"42"},
			"Etag":           {"etag"},
			"Last-Modified":  {(time.Time{}).Add(24 * time.Hour).Format(time.RFC1123)},
		},
	})

	mBlobAPI.On("GetProperties", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

	mBlockAPI.On("URL").Return(url.URL{Host: "example.com"})

	fn1 := func(sum []byte) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(sum, expected[:])
	}

	mBlockAPI.On(
		"StageBlock",
		mock.Anything,
		mock.Anything,
		strings.NewReader("value"),
		mock.Anything,
		mock.MatchedBy(fn1),
		mock.Anything,
	).Return(nil, nil)

	fn2 := func(blob string) bool {
		_, err := base64.StdEncoding.DecodeString(blob)
		return err == nil
	}

	mBlockAPI.On(
		"StageBlockFromURL",
		mock.Anything,
		mock.MatchedBy(fn2),
		mock.Anything,
		mock.MatchedBy(func(offset int64) bool { return offset == 0 }),
		mock.MatchedBy(func(length int64) bool { return length == azblob.CountToEnd }),
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, nil)

	fn3 := func(parts []string) bool {
		return len(parts) == 2 && slice.ContainsString(parts, "blob")
	}

	mBlockAPI.On(
		"CommitBlockList",
		mock.Anything,
		mock.MatchedBy(fn3),
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.AppendToObject("container", "blob", strings.NewReader("value")))
}

func TestClientDeleteObjects(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
		mbAPI = &mockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob1" })).Return(mbAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob2" })).Return(mbAPI)

	mbAPI.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

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
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob" })).Return(mbAPI)

	mbAPI.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(
		nil,
		&mockError{inner: azblob.ServiceCodeBlobNotFound},
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
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	output := &azblob.ListBlobsFlatSegmentResponse{
		NextMarker: azblob.Marker{Val: aws.String("")},
		Segment: azblob.BlobFlatListSegment{BlobItems: []azblob.BlobItemInternal{
			{
				Name: "blob1",
				Properties: azblob.BlobProperties{
					ContentLength: aws.Int64(64),
					LastModified:  (time.Time{}).Add(24 * time.Hour),
				},
			},
			{
				Name: "blob2",
				Properties: azblob.BlobProperties{
					ContentLength: aws.Int64(128),
					LastModified:  (time.Time{}).Add(48 * time.Hour),
				},
			},
		}},
	}

	mcAPI.On(
		"ListBlobsFlatSegment",
		mock.Anything,
		mock.MatchedBy(func(marker azblob.Marker) bool { return marker.Val == nil }),
		mock.Anything,
	).Return(output, nil)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob1" })).Return(mbAPI)

	mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "blob2" })).Return(mbAPI)

	mbAPI.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	client := &Client{storageAPI: msAPI}

	err := client.DeleteDirectory("container", "")
	require.NoError(t, err)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 3)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ListBlobsFlatSegment", 1)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 2)

	mbAPI.AssertExpectations(t)
	mbAPI.AssertNumberOfCalls(t, "Delete", 2)
}

func TestClientIterateObjects(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	output := &azblob.ListBlobsFlatSegmentResponse{
		NextMarker: azblob.Marker{Val: aws.String("")},
		Segment:    azblob.BlobFlatListSegment{BlobItems: make([]azblob.BlobItemInternal, 0)},
	}

	mcAPI.On(
		"ListBlobsFlatSegment",
		mock.Anything,
		mock.MatchedBy(func(marker azblob.Marker) bool { return marker.Val == nil }),
		mock.MatchedBy(func(options azblob.ListBlobsSegmentOptions) bool { return options.Prefix == "prefix" }),
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	require.NoError(t, client.IterateObjects("container", "prefix", nil, nil, nil))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ListBlobsFlatSegment", 1)
}

func TestClientIterateObjectsBothIncludeExcludeSupplied(t *testing.T) {
	client := &Client{}

	err := client.IterateObjects("bucket", "prefix", []*regexp.Regexp{}, []*regexp.Regexp{}, nil)
	require.ErrorIs(t, err, objcli.ErrIncludeAndExcludeAreMutuallyExclusive)
}

func TestClientIterateObjectsPropagateUserError(t *testing.T) {
	var (
		msAPI = &mockBlobStorageAPI{}
		mcAPI = &mockContainerAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	output := &azblob.ListBlobsFlatSegmentResponse{
		NextMarker: azblob.Marker{Val: aws.String("")},
		Segment: azblob.BlobFlatListSegment{BlobItems: []azblob.BlobItemInternal{
			{
				Name: "blob1",
				Properties: azblob.BlobProperties{
					ContentLength: aws.Int64(42),
					LastModified:  (time.Time{}).Add(24 * time.Hour),
				},
			},
		}},
	}

	mcAPI.On(
		"ListBlobsFlatSegment",
		mock.Anything,
		mock.MatchedBy(func(marker azblob.Marker) bool { return marker.Val == nil }),
		mock.MatchedBy(func(options azblob.ListBlobsSegmentOptions) bool { return options.Prefix == "prefix" }),
	).Return(output, nil)

	client := &Client{storageAPI: msAPI}

	err := client.IterateObjects("container", "prefix", nil, nil, func(attrs *objval.ObjectAttrs) error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ListBlobsFlatSegment", 1)
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
			)

			msAPI.On("ToContainerAPI", mock.MatchedBy(
				func(container string) bool { return container == "container" })).Return(mcAPI)

			blobs := []string{
				"/path/to/blob1",
				"/path/to/another/blob1",
				"/path/to/blob2",
				"", // Final call should return an empty name (marker) which indicates the end of iteration
			}

			for i := int64(1); i <= 3; i++ {
				output := &azblob.ListBlobsFlatSegmentResponse{
					NextMarker: azblob.Marker{Val: aws.String(blobs[i])},
					Segment: azblob.BlobFlatListSegment{BlobItems: []azblob.BlobItemInternal{
						{
							Name: blobs[i-1],
							Properties: azblob.BlobProperties{
								ContentLength: aws.Int64(64 << (i - 1)),
								LastModified:  (time.Time{}).Add(time.Duration(i*24) * time.Hour),
							},
						},
					}},
				}

				var expected *string
				if i > 1 {
					expected = aws.String(blobs[i-1])
				}

				call := mcAPI.On(
					"ListBlobsFlatSegment",
					mock.Anything,
					mock.MatchedBy(func(marker azblob.Marker) bool { return reflect.DeepEqual(marker.Val, expected) }),
					mock.Anything,
				).Return(output, nil)

				call.Repeatability = 1
			}

			output := &azblob.ListBlobsFlatSegmentResponse{
				NextMarker: azblob.Marker{Val: aws.String("")},
			}

			mcAPI.On("ListBlobsFlatSegment", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

			client := &Client{storageAPI: msAPI}

			var all []*objval.ObjectAttrs

			err := client.IterateObjects("container", "", test.include, test.exclude, func(attrs *objval.ObjectAttrs) error {
				all = append(all, attrs)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, test.all, all)

			msAPI.AssertExpectations(t)
			msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

			mcAPI.AssertExpectations(t)
			mcAPI.AssertNumberOfCalls(t, "ListBlobsFlatSegment", 3)
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

	_, err := client.UploadPart("container", "blob", "id", 42, nil)
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientUploadPart(t *testing.T) {
	var (
		msAPI     = &mockBlobStorageAPI{}
		mcAPI     = &mockContainerAPI{}
		mBlobAPI  = &mockBlobAPI{}
		mBlockAPI = &mockBlockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.Anything).Return(mBlobAPI)

	mBlobAPI.On("ToBlockBlobAPI").Return(mBlockAPI)

	fn1 := func(sum []byte) bool {
		expected := md5.Sum([]byte("value"))

		return bytes.Equal(sum, expected[:])
	}

	mBlockAPI.On(
		"StageBlock",
		mock.Anything,
		mock.Anything,
		strings.NewReader("value"),
		mock.Anything,
		mock.MatchedBy(fn1),
		mock.Anything,
	).Return(nil, nil)

	client := &Client{storageAPI: msAPI}

	part, err := client.UploadPart("container", objcli.NoUploadID, "blob", 42, strings.NewReader("value"))
	require.NoError(t, err)
	require.NotZero(t, part.ID)

	_, err = base64.StdEncoding.DecodeString(part.ID)
	require.NoError(t, err)

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mBlobAPI.AssertExpectations(t)
	mBlobAPI.AssertNumberOfCalls(t, "ToBlockBlobAPI", 1)

	mBlockAPI.AssertExpectations(t)
	mBlockAPI.AssertNumberOfCalls(t, "StageBlock", 1)
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
			eLength: 129,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				msAPI = &mockBlobStorageAPI{}
				mcAPI = &mockContainerAPI{}

				mSrcBlobAPI  = &mockBlobAPI{}
				mSrcBlockAPI = &mockBlockBlobAPI{}

				mDstBlobAPI  = &mockBlobAPI{}
				mDstBlockAPI = &mockBlockBlobAPI{}
			)

			msAPI.On("ToContainerAPI", mock.MatchedBy(
				func(container string) bool { return container == "container" })).Return(mcAPI)

			mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "dst" })).Return(mDstBlobAPI)

			mcAPI.On("ToBlobAPI", mock.MatchedBy(func(blob string) bool { return blob == "src" })).Return(mSrcBlobAPI)

			mDstBlobAPI.On("ToBlockBlobAPI").Return(mDstBlockAPI)

			mSrcBlobAPI.On("ToBlockBlobAPI").Return(mSrcBlockAPI)

			mSrcBlockAPI.On("URL").Return(url.URL{Host: "example.com"})

			fn1 := func(blob string) bool {
				_, err := base64.StdEncoding.DecodeString(blob)
				return err == nil
			}

			fn2 := func(u url.URL) bool {
				return reflect.DeepEqual(url.URL{Host: "example.com"}, u)
			}

			mDstBlockAPI.On(
				"StageBlockFromURL",
				mock.Anything,
				mock.MatchedBy(fn1),
				mock.MatchedBy(fn2),
				mock.MatchedBy(func(offset int64) bool { return offset == test.eOffset }),
				mock.MatchedBy(func(length int64) bool { return length == test.eLength }),
				mock.Anything,
				mock.Anything,
				mock.Anything,
			).Return(nil, nil)

			client := &Client{storageAPI: msAPI}

			part, err := client.UploadPartCopy("container", objcli.NoUploadID, "dst", "src", 42, test.br)
			require.NoError(t, err)
			require.NotZero(t, part.ID)

			_, err = base64.StdEncoding.DecodeString(part.ID)
			require.NoError(t, err)

			msAPI.AssertExpectations(t)
			msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

			mcAPI.AssertExpectations(t)
			mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 2)

			mDstBlobAPI.AssertExpectations(t)
			mDstBlobAPI.AssertNumberOfCalls(t, "ToBlockBlobAPI", 1)

			mSrcBlobAPI.AssertExpectations(t)
			mSrcBlobAPI.AssertNumberOfCalls(t, "ToBlockBlobAPI", 1)

			mDstBlockAPI.AssertExpectations(t)
			mDstBlockAPI.AssertNumberOfCalls(t, "StageBlockFromURL", 1)

			mSrcBlockAPI.AssertExpectations(t)
			mSrcBlockAPI.AssertNumberOfCalls(t, "URL", 1)
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

	err := client.CompleteMultipartUpload("bucket", "id", "key", objval.Part{})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadOverMaxComposable(t *testing.T) {
	var (
		msAPI     = &mockBlobStorageAPI{}
		mcAPI     = &mockContainerAPI{}
		mBlobAPI  = &mockBlobAPI{}
		mBlockAPI = &mockBlockBlobAPI{}
	)

	msAPI.On("ToContainerAPI", mock.MatchedBy(
		func(container string) bool { return container == "container" })).Return(mcAPI)

	mcAPI.On("ToBlobAPI", mock.Anything).Return(mBlobAPI)

	mBlobAPI.On("ToBlockBlobAPI").Return(mBlockAPI)

	fn1 := func(parts []string) bool {
		return slice.EqualStrings(parts, []string{"blob1", "blob2", "blob3"})
	}

	mBlockAPI.On(
		"CommitBlockList",
		mock.Anything,
		mock.MatchedBy(fn1),
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, nil)

	client := &Client{storageAPI: msAPI}

	parts := make([]objval.Part, 0)

	for i := 1; i <= 3; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("blob%d", i), Number: i})
	}

	require.NoError(t, client.CompleteMultipartUpload("container", objcli.NoUploadID, "blob", parts...))

	msAPI.AssertExpectations(t)
	msAPI.AssertNumberOfCalls(t, "ToContainerAPI", 1)

	mcAPI.AssertExpectations(t)
	mcAPI.AssertNumberOfCalls(t, "ToBlobAPI", 1)

	mBlobAPI.AssertExpectations(t)
	mBlobAPI.AssertNumberOfCalls(t, "ToBlockBlobAPI", 1)

	mBlockAPI.AssertExpectations(t)
	mBlockAPI.AssertNumberOfCalls(t, "CommitBlockList", 1)
}

func TestClientAbortMultipartUpload(t *testing.T) {
	client := &Client{}

	require.NoError(t, client.AbortMultipartUpload("container", objcli.NoUploadID, "blob", objval.Part{}))
}

func TestClientAbortMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.AbortMultipartUpload("container", "id", "blob", objval.Part{})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}
