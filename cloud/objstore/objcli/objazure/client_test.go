package objazure

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	"github.com/couchbase/tools-common/testing/mock/matchers"
	"github.com/couchbase/tools-common/types/v2/ptr"
	"github.com/couchbase/tools-common/types/v2/timeprovider"
)

type customMatcher struct {
	message string
	match   func(arg any) bool
}

func (o customMatcher) Matches(x interface{}) bool {
	return o.match(x)
}

func (o customMatcher) String() string {
	return o.message
}

func immutabilityModeMatcher(mode blob.ImmutabilityPolicySetting) customMatcher {
	return customMatcher{
		message: fmt.Sprintf("Immutability policy mode should be %q", mode),
		match: func(arg interface{}) bool {
			var immutabilityPolicyMode *blob.ImmutabilityPolicySetting

			switch inputOpts := arg.(type) {
			case *blob.SetImmutabilityPolicyOptions:
				immutabilityPolicyMode = inputOpts.Mode
			default:
				return false
			}

			return immutabilityPolicyMode != nil && *immutabilityPolicyMode == mode
		},
	}
}

func immutabilityPolicyMatcher(mode blob.ImmutabilityPolicySetting, expiryTime time.Time) customMatcher {
	return customMatcher{
		message: fmt.Sprintf("Immutability policy mode should be %q and expiry time should be %q", mode, expiryTime),
		match: func(arg interface{}) bool {
			var immutabilityPolicyMode *blob.ImmutabilityPolicySetting
			var immutabilityPolicyExpiryTime *time.Time

			switch inputOpts := arg.(type) {
			case *blockblob.UploadOptions:
				immutabilityPolicyMode = inputOpts.ImmutabilityPolicyMode
				immutabilityPolicyExpiryTime = inputOpts.ImmutabilityPolicyExpiryTime
			case *blockblob.CommitBlockListOptions:
				immutabilityPolicyMode = inputOpts.ImmutabilityPolicyMode
				immutabilityPolicyExpiryTime = inputOpts.ImmutabilityPolicyExpiryTime
			default:
				return false
			}

			modeMatch := immutabilityPolicyMode != nil && *immutabilityPolicyMode == mode
			expiryMatch := immutabilityPolicyExpiryTime != nil && *immutabilityPolicyExpiryTime == expiryTime

			return modeMatch && expiryMatch
		},
	}
}

func ifAbsentMatcher() customMatcher {
	return customMatcher{
		message: fmt.Sprintf("ifNoneMatch should be %q ", azcore.ETagAny),
		match: func(arg interface{}) bool {
			var accessConditions *blob.AccessConditions

			switch inputOpts := arg.(type) {
			case *blockblob.UploadOptions:
				accessConditions = inputOpts.AccessConditions
			case *blockblob.CommitBlockListOptions:
				accessConditions = inputOpts.AccessConditions
			default:
				return false
			}

			if accessConditions == nil || accessConditions.ModifiedAccessConditions == nil {
				return false
			}

			ifNoneMatch := accessConditions.ModifiedAccessConditions.IfNoneMatch

			return ifNoneMatch != nil && *ifNoneMatch == azcore.ETagAny
		},
	}
}

func TestNewClient(t *testing.T) {
	require.Equal(
		t,
		&Client{serviceAPI: &serviceClient{}, timeProvider: timeprovider.CurrentTimeProvider{}},
		NewClient(ClientOptions{}),
	)
}

func TestClientProvider(t *testing.T) {
	require.Equal(t, objval.ProviderAzure, (&Client{}).Provider())
}

type testEnvironment struct {
	ctrl         *gomock.Controller
	client       *Client
	serviceAPI   *MockserviceAPI
	containerAPI *MockcontainerAPI
	blobAPI      *MockblobAPI
	blockBlobAPI *MockblockBlobAPI
}

func newTestEnvironment(t *testing.T) testEnvironment {
	var (
		ctrl  = gomock.NewController(t)
		sAPI  = NewMockserviceAPI(ctrl)
		cAPI  = NewMockcontainerAPI(ctrl)
		bAPI  = NewMockblobAPI(ctrl)
		bbAPI = NewMockblockBlobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient(gomock.Any()).Return(cAPI).AnyTimes()
	cAPI.EXPECT().NewBlockBlobClient(gomock.Any()).Return(bbAPI).AnyTimes()
	cAPI.EXPECT().NewBlobClient(gomock.Any()).Return(bAPI).AnyTimes()

	return testEnvironment{
		ctrl:         ctrl,
		client:       &Client{serviceAPI: sAPI, timeProvider: timeprovider.CurrentTimeProvider{}},
		serviceAPI:   sAPI,
		containerAPI: cAPI,
		blobAPI:      bAPI,
		blockBlobAPI: bbAPI,
	}
}

func TestClientGetObject(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blob.DownloadStreamResponse{}

	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = ptr.To[int64](42)
	output.Body = io.NopCloser(strings.NewReader("value"))

	bAPI.
		EXPECT().
		DownloadStream(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			require.Equal(t, int64(0), opts.Range.Count)
			require.Equal(t, int64(0), opts.Range.Offset)

			return output, nil
		})

	object, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "container",
		Key:    "blob",
	})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         ptr.To[int64](42),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blob.DownloadStreamResponse{}

	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = ptr.To[int64](42)
	output.Body = io.NopCloser(strings.NewReader("value"))

	bAPI.
		EXPECT().
		DownloadStream(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			require.Equal(t, int64(65), opts.Range.Count)
			require.Equal(t, int64(64), opts.Range.Offset)

			return output, nil
		})

	object, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket:    "container",
		Key:       "blob",
		ByteRange: &objval.ByteRange{Start: 64, End: 128},
	})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         ptr.To[int64](42),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket:    "bucket",
		Key:       "blob",
		ByteRange: &objval.ByteRange{Start: 128, End: 64},
	})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientGetObjectVersionID(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	cAPI := testEnvironment.containerAPI
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blob.DownloadStreamResponse{}

	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = ptr.To[int64](42)
	output.Body = io.NopCloser(strings.NewReader("value"))
	output.VersionID = ptr.To("version1")

	cAPI.EXPECT().NewBlockBlobVersionClient("blob", "version1").Return(bAPI, nil)

	bAPI.
		EXPECT().
		DownloadStream(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			require.Equal(t, int64(0), opts.Range.Count)
			require.Equal(t, int64(0), opts.Range.Offset)

			return output, nil
		})

	object, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket:    "container",
		Key:       "blob",
		VersionID: "version1",
	})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         ptr.To[int64](42),
			LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
			VersionID:    "version1",
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)
}

func TestClientGetObjectLockData(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	now := time.Now()

	output := blob.DownloadStreamResponse{}

	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = ptr.To[int64](42)
	output.Body = io.NopCloser(strings.NewReader("value"))
	output.ImmutabilityPolicyExpiresOn = &now
	output.ImmutabilityPolicyMode = ptr.To(blob.ImmutabilityPolicyModeLocked)

	bAPI.
		EXPECT().
		DownloadStream(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			require.Equal(t, int64(0), opts.Range.Count)
			require.Equal(t, int64(0), opts.Range.Offset)

			return output, nil
		})

	object, err := client.GetObject(context.Background(), objcli.GetObjectOptions{
		Bucket: "container",
		Key:    "blob",
	})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:            "blob",
			Size:           ptr.To[int64](42),
			LastModified:   ptr.To((time.Time{}).Add(24 * time.Hour)),
			LockExpiration: &now,
			LockType:       objval.LockTypeCompliance,
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)
}

func TestClientGetObjectAttrs(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blob.GetPropertiesResponse{}

	output.ContentLength = ptr.To[int64](42)
	output.ETag = ptr.To(azcore.ETag("etag"))
	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))

	bAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	attrs, err := client.GetObjectAttrs(context.Background(), objcli.GetObjectAttrsOptions{
		Bucket: "container",
		Key:    "blob",
	})
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "blob",
		ETag:         ptr.To("etag"),
		Size:         ptr.To[int64](42),
		LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
	}

	require.Equal(t, expected, attrs)
}

func TestClientGetObjectAttrsVersionID(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	cAPI := testEnvironment.containerAPI
	bAPI := testEnvironment.blockBlobAPI

	output := blob.GetPropertiesResponse{}

	output.ContentLength = ptr.To[int64](42)
	output.ETag = ptr.To(azcore.ETag("etag"))
	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))
	output.VersionID = ptr.To("version1")

	cAPI.EXPECT().NewBlockBlobVersionClient("blob", "version1").Return(bAPI, nil)

	bAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	attrs, err := client.GetObjectAttrs(context.Background(), objcli.GetObjectAttrsOptions{
		Bucket:    "container",
		Key:       "blob",
		VersionID: "version1",
	})
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "blob",
		ETag:         ptr.To("etag"),
		Size:         ptr.To[int64](42),
		LastModified: ptr.To((time.Time{}).Add(24 * time.Hour)),
		VersionID:    "version1",
	}

	require.Equal(t, expected, attrs)
}

func TestClientGetObjectAttrsLockData(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	now := time.Now()

	output := blob.GetPropertiesResponse{}

	output.ContentLength = ptr.To[int64](42)
	output.ETag = ptr.To(azcore.ETag("etag"))
	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))
	output.ImmutabilityPolicyExpiresOn = &now
	output.ImmutabilityPolicyMode = ptr.To(blob.ImmutabilityPolicyModeLocked)

	bAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	attrs, err := client.GetObjectAttrs(context.Background(), objcli.GetObjectAttrsOptions{
		Bucket: "container",
		Key:    "blob",
	})
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:            "blob",
		ETag:           ptr.To("etag"),
		Size:           ptr.To[int64](42),
		LastModified:   ptr.To((time.Time{}).Add(24 * time.Hour)),
		LockType:       objval.LockTypeCompliance,
		LockExpiration: &now,
	}

	require.Equal(t, expected, attrs)
}

func TestClientPutObject(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blockblob.UploadResponse{}

	fn := func(
		_ context.Context, _ io.ReadSeekCloser, opts *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error) {
		b := md5.Sum([]byte("value"))
		require.Equal(t, blob.TransferValidationTypeMD5(b[:]), opts.TransactionalValidation)

		return output, nil
	}

	bAPI.
		EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(fn)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "container",
		Key:    "blob",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)
}

func TestClientPutObjectLockData(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	now := time.Now()
	expirationTime := now.AddDate(0, 0, 5)

	fn := func(
		_ context.Context, _ io.ReadSeekCloser, opts *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error) {
		b := md5.Sum([]byte("value"))
		require.Equal(t, blob.TransferValidationTypeMD5(b[:]), opts.TransactionalValidation)

		return blockblob.UploadResponse{}, nil
	}

	bAPI.
		EXPECT().
		Upload(
			gomock.Any(),
			gomock.Any(),
			immutabilityPolicyMatcher(blob.ImmutabilityPolicySettingLocked, expirationTime),
		).DoAndReturn(fn)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "container",
		Key:    "blob",
		Body:   strings.NewReader("value"),
		Lock:   objcli.NewComplianceLock(expirationTime),
	})
	require.NoError(t, err)
}

func TestClientPutObjectIfAbsent(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blockblob.UploadResponse{}

	fn := func(
		_ context.Context, _ io.ReadSeekCloser, opts *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error) {
		b := md5.Sum([]byte("value"))
		require.Equal(t, blob.TransferValidationTypeMD5(b[:]), opts.TransactionalValidation)

		return output, nil
	}

	bAPI.
		EXPECT().
		Upload(gomock.Any(), gomock.Any(), ifAbsentMatcher()).
		DoAndReturn(fn)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket:       "container",
		Key:          "blob",
		Body:         strings.NewReader("value"),
		Precondition: objcli.OperationPreconditionOnlyIfAbsent,
	})
	require.NoError(t, err)
}

func TestClientAppendToObjectNotExists(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	bAPI.
		EXPECT().
		GetProperties(gomock.Any(), gomock.Any()).
		Return(blob.GetPropertiesResponse{}, &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)})

	output := blockblob.UploadResponse{}

	fn := func(
		_ context.Context, _ io.ReadSeekCloser, opts *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error) {
		b := md5.Sum([]byte("value"))
		require.Equal(t, blob.TransferValidationTypeMD5(b[:]), opts.TransactionalValidation)

		return output, nil
	}

	bAPI.
		EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(fn)

	err := client.AppendToObject(context.Background(), objcli.AppendToObjectOptions{
		Bucket: "container",
		Key:    "blob",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)
}

func TestClientCopyObject(t *testing.T) {
	var (
		ctrl  = gomock.NewController(t)
		sAPI  = NewMockserviceAPI(ctrl)
		scAPI = NewMockcontainerAPI(ctrl)
		dcAPI = NewMockcontainerAPI(ctrl)
		dbAPI = NewMockblobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient("srcContainer").Return(scAPI)
	sAPI.EXPECT().NewContainerClient("dstContainer").Return(dcAPI)

	scAPI.EXPECT().NewBlobClient("srcBlob").Return(dbAPI)
	dcAPI.EXPECT().NewBlobClient("dstBlob").Return(dbAPI)

	dbAPI.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("example.com", nil)

	client := &Client{serviceAPI: sAPI, timeProvider: timeprovider.CurrentTimeProvider{}}

	fn := func(
		_ context.Context,
		_ string,
		_ *blob.CopyFromURLOptions,
	) (blob.CopyFromURLResponse, error) {
		return blob.CopyFromURLResponse{}, nil
	}

	dbAPI.
		EXPECT().
		CopyFromURL(matchers.Context, gomock.Any(), gomock.Any()).
		DoAndReturn(fn)

	err := client.CopyObject(context.Background(), objcli.CopyObjectOptions{
		DestinationBucket: "dstContainer",
		DestinationKey:    "dstBlob",
		SourceBucket:      "srcContainer",
		SourceKey:         "srcBlob",
	})
	require.NoError(t, err)
}

func TestClientAppendToObject(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI
	blobClient := testEnvironment.blobAPI

	output := blob.GetPropertiesResponse{}

	output.ContentLength = ptr.To[int64](42)
	output.ETag = ptr.To(azcore.ETag("etag"))
	output.LastModified = ptr.To((time.Time{}).Add(24 * time.Hour))

	blobClient.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("example.com", nil)

	bAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	bAPI.
		EXPECT().
		StageBlockFromURL(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, _, _ string, options *blockblob.StageBlockFromURLOptions,
		) (blockblob.StageBlockFromURLResponse, error) {
			require.Equal(t, int64(0), options.Range.Offset)
			require.Equal(t, int64(blob.CountToEnd), options.Range.Count)

			return blockblob.StageBlockFromURLResponse{}, nil
		})

	bAPI.
		EXPECT().
		StageBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(blockblob.StageBlockResponse{}, nil)

	bAPI.
		EXPECT().
		CommitBlockList(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, base64BlockIDs []string, _ *blockblob.CommitBlockListOptions,
		) (blockblob.CommitBlockListResponse, error) {
			require.Len(t, base64BlockIDs, 2)
			return blockblob.CommitBlockListResponse{}, nil
		})

	err := client.AppendToObject(context.Background(), objcli.AppendToObjectOptions{
		Bucket: "container",
		Key:    "blob",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)
}

func TestClientDeleteObjects(t *testing.T) {
	var (
		ctrl = gomock.NewController(t)
		sAPI = NewMockserviceAPI(ctrl)
		cAPI = NewMockcontainerAPI(ctrl)
		bAPI = NewMockblockBlobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient("container").Return(cAPI).Times(2)
	cAPI.EXPECT().NewBlockBlobClient("blob1").Return(bAPI)
	cAPI.EXPECT().NewBlockBlobClient("blob2").Return(bAPI)

	client := &Client{serviceAPI: sAPI}

	bAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(blob.DeleteResponse{}, nil).Times(2)

	err := client.DeleteObjects(context.Background(), objcli.DeleteObjectsOptions{
		Bucket: "container",
		Keys:   []string{"blob1", "blob2"},
	})
	require.NoError(t, err)
}

func TestClientDeleteObjectsKeyNotFound(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	bAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(
		blob.DeleteResponse{},
		&azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)},
	)

	err := client.DeleteObjects(context.Background(), objcli.DeleteObjectsOptions{
		Bucket: "container",
		Keys:   []string{"blob"},
	})
	require.NoError(t, err)
}

func TestClientDeleteObjectVersions(t *testing.T) {
	var (
		ctrl = gomock.NewController(t)
		sAPI = NewMockserviceAPI(ctrl)
		cAPI = NewMockcontainerAPI(ctrl)
		bAPI = NewMockblockBlobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient("container").Return(cAPI).Times(4)
	cAPI.EXPECT().NewBlockBlobVersionClient("blob1", "v1").Return(bAPI, nil)
	cAPI.EXPECT().NewBlockBlobVersionClient("blob2", "v1").Return(bAPI, nil)
	cAPI.EXPECT().NewBlockBlobVersionClient("blob1", "v2").Return(bAPI, nil)
	cAPI.EXPECT().NewBlockBlobVersionClient("blob2", "v2").Return(bAPI, nil)

	client := &Client{serviceAPI: sAPI}

	bAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(blob.DeleteResponse{}, nil).Times(4)

	err := client.DeleteObjectVersions(context.Background(), objcli.DeleteObjectVersionsOptions{
		Bucket: "container",
		Versions: []objval.ObjectVersion{
			{Key: "blob1", VersionID: "v1"},
			{Key: "blob2", VersionID: "v1"},
			{Key: "blob1", VersionID: "v2"},
			{Key: "blob2", VersionID: "v2"},
		},
	})
	require.NoError(t, err)
}

func TestClientDeleteObjectVersionsKeyNotFound(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	cAPI := testEnvironment.containerAPI
	bAPI := testEnvironment.blockBlobAPI

	cAPI.EXPECT().NewBlockBlobVersionClient("blob1", "v1").Return(bAPI, nil)

	bAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(
		blob.DeleteResponse{},
		&azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)},
	)

	err := client.DeleteObjectVersions(context.Background(), objcli.DeleteObjectVersionsOptions{
		Bucket:   "container",
		Versions: []objval.ObjectVersion{{Key: "blob1", VersionID: "v1"}},
	})
	require.NoError(t, err)
}

func TestClientCreateMultipartUpload(t *testing.T) {
	client := &Client{}

	id, err := client.CreateMultipartUpload(context.Background(), objcli.CreateMultipartUploadOptions{
		Bucket: "container",
		Key:    "blob",
	})
	require.NoError(t, err)
	require.Equal(t, objcli.NoUploadID, id)
}

func TestClientUploadPartWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPart(context.Background(), objcli.UploadPartOptions{
		Bucket:   "container",
		UploadID: "id",
		Key:      "blob",
		Number:   42,
	})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientListParts(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	output := blockblob.GetBlockListResponse{}
	output.BlockList = blockblob.BlockList{
		UncommittedBlocks: []*blockblob.Block{
			{
				Name: ptr.To("block3"),
				Size: ptr.To[int64](256),
			},
			{
				Name: ptr.To("block4"),
				Size: ptr.To[int64](512),
			},
		},
	}

	bAPI.EXPECT().GetBlockList(gomock.Any(), gomock.Any(), gomock.Any()).Return(output, nil)

	parts, err := client.ListParts(context.Background(), objcli.ListPartsOptions{
		Bucket:   "container",
		UploadID: objcli.NoUploadID,
		Key:      "blob",
	})
	require.NoError(t, err)

	expected := []objval.Part{{ID: "block3", Size: 256}, {ID: "block4", Size: 512}}
	require.Equal(t, expected, parts)
}

func TestClientListPartsWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.ListParts(context.Background(), objcli.ListPartsOptions{
		Bucket:   "container",
		UploadID: "id",
		Key:      "blob",
	})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientUploadPart(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	bAPI.
		EXPECT().
		StageBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, _ string, _ io.ReadSeekCloser, _ *blockblob.StageBlockOptions,
		) (blockblob.StageBlockResponse, error) {
			return blockblob.StageBlockResponse{}, nil
		})

	part, err := client.UploadPart(context.Background(), objcli.UploadPartOptions{
		Bucket:   "container",
		UploadID: objcli.NoUploadID,
		Key:      "blob",
		Number:   42,
		Body:     strings.NewReader("value"),
	})
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
}

func TestClientUploadPartCopyWithSASToken(t *testing.T) {
	type test struct {
		name    string
		br      *objval.ByteRange
		eOffset int64
		eLength int64
	}

	tests := []*test{
		{
			name:    "NoByteRange",
			eLength: blob.CountToEnd,
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
				ctrl   = gomock.NewController(t)
				sAPI   = NewMockserviceAPI(ctrl)
				cAPI   = NewMockcontainerAPI(ctrl)
				srcAPI = NewMockblobAPI(ctrl)
				dstAPI = NewMockblockBlobAPI(ctrl)
			)

			sAPI.EXPECT().NewContainerClient("container").Return(cAPI).Times(2)
			cAPI.EXPECT().NewBlobClient("src").Return(srcAPI)
			cAPI.EXPECT().NewBlockBlobClient("dst").Return(dstAPI)

			client := Client{serviceAPI: sAPI, timeProvider: timeprovider.CurrentTimeProvider{}}

			srcAPI.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("example.com", nil)

			dstAPI.
				EXPECT().
				StageBlockFromURL(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context, base64BlockID, _ string, options *blockblob.StageBlockFromURLOptions,
				) (blockblob.StageBlockFromURLResponse, error) {
					require.Equal(t, test.eOffset, options.Range.Offset)
					require.Equal(t, test.eLength, options.Range.Count)

					_, err := base64.StdEncoding.DecodeString(base64BlockID)
					require.NoError(t, err)

					return blockblob.StageBlockFromURLResponse{}, nil
				})

			part, err := client.UploadPartCopy(context.Background(), objcli.UploadPartCopyOptions{
				DestinationBucket: "container",
				UploadID:          objcli.NoUploadID,
				DestinationKey:    "dst",
				SourceBucket:      "container",
				SourceKey:         "src",
				Number:            42,
				ByteRange:         test.br,
			})
			require.NoError(t, err)
			require.NotZero(t, part.ID)

			_, err = base64.StdEncoding.DecodeString(part.ID)
			require.NoError(t, err)
		})
	}
}

func TestClientUploadPartCopy(t *testing.T) {
	var (
		ctrl            = gomock.NewController(t)
		sAPI            = NewMockserviceAPI(ctrl)
		srcContainerAPI = NewMockcontainerAPI(ctrl)
		dstContainerAPI = NewMockcontainerAPI(ctrl)
		srcBlockBlobAPI = NewMockblockBlobAPI(ctrl)
		srcBlobAPI      = NewMockblobAPI(ctrl)
		dstBlockBlobAPI = NewMockblockBlobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient("srcContainer").Return(srcContainerAPI).Times(2)
	sAPI.EXPECT().NewContainerClient("dstContainer").Return(dstContainerAPI)
	srcContainerAPI.EXPECT().NewBlockBlobClient("srcKey").Return(srcBlockBlobAPI)
	dstContainerAPI.EXPECT().NewBlockBlobClient("dstKey").Return(dstBlockBlobAPI)
	srcContainerAPI.EXPECT().NewBlobClient("srcKey").Return(srcBlobAPI)

	srcBlobAPI.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("", errors.New(sasErrString))
	srcBlockBlobAPI.EXPECT().URL().Return("example.com")

	client := Client{serviceAPI: sAPI, timeProvider: timeprovider.CurrentTimeProvider{}}

	dstBlockBlobAPI.
		EXPECT().
		StageBlockFromURL(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, base64BlockID, _ string, options *blockblob.StageBlockFromURLOptions,
		) (blockblob.StageBlockFromURLResponse, error) {
			require.Equal(t, int64(0), options.Range.Count)

			_, err := base64.StdEncoding.DecodeString(base64BlockID)
			require.NoError(t, err)

			return blockblob.StageBlockFromURLResponse{}, nil
		})

	part, err := client.UploadPartCopy(context.Background(), objcli.UploadPartCopyOptions{
		DestinationBucket: "dstContainer",
		UploadID:          objcli.NoUploadID,
		DestinationKey:    "dstKey",
		SourceBucket:      "srcContainer",
		SourceKey:         "srcKey",
		Number:            42,
	})
	require.NoError(t, err)
	require.NotZero(t, part.ID)

	_, err = base64.StdEncoding.DecodeString(part.ID)
	require.NoError(t, err)
}

func TestClientUploadPartCopyWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy(context.Background(), objcli.UploadPartCopyOptions{
		DestinationBucket: "dstContainer",
		UploadID:          objcli.NoUploadID,
		DestinationKey:    "dstKey",
		SourceBucket:      "srcContainer",
		SourceKey:         "srcKey",
		Number:            42,
		ByteRange:         &objval.ByteRange{Start: 128, End: 64},
	})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientUploadPartCopyWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy(context.Background(), objcli.UploadPartCopyOptions{
		DestinationBucket: "dstContainer",
		UploadID:          "id",
		DestinationKey:    "dstKey",
		SourceBucket:      "srcContainer",
		SourceKey:         "srcKey",
		Number:            42,
	})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.CompleteMultipartUpload(context.Background(), objcli.CompleteMultipartUploadOptions{
		Bucket:   "bucket",
		UploadID: "id",
		Key:      "blob",
	})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadOverMaxComposable(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	bAPI.
		EXPECT().
		CommitBlockList(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, base64BlockIDs []string, _ *blockblob.CommitBlockListOptions,
		) (blockblob.CommitBlockListResponse, error) {
			require.Equal(t, []string{"blob1", "blob2", "blob3"}, base64BlockIDs)

			return blockblob.CommitBlockListResponse{}, nil
		})

	parts := make([]objval.Part, 0)

	for i := 1; i <= 3; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("blob%d", i), Number: i})
	}

	err := client.CompleteMultipartUpload(context.Background(), objcli.CompleteMultipartUploadOptions{
		Bucket:   "container",
		UploadID: objcli.NoUploadID,
		Key:      "blob",
		Parts:    parts,
	})
	require.NoError(t, err)
}

func TestClientCompleteMultipartUploadIfAbsent(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	bAPI.
		EXPECT().
		CommitBlockList(gomock.Any(), gomock.Any(), ifAbsentMatcher()).
		DoAndReturn(func(
			_ context.Context, base64BlockIDs []string, _ *blockblob.CommitBlockListOptions,
		) (blockblob.CommitBlockListResponse, error) {
			require.Equal(t, []string{"blob1", "blob2", "blob3"}, base64BlockIDs)

			return blockblob.CommitBlockListResponse{}, nil
		})

	parts := make([]objval.Part, 0)

	for i := 1; i <= 3; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("blob%d", i), Number: i})
	}

	err := client.CompleteMultipartUpload(context.Background(), objcli.CompleteMultipartUploadOptions{
		Bucket:       "container",
		UploadID:     objcli.NoUploadID,
		Key:          "blob",
		Parts:        parts,
		Precondition: objcli.OperationPreconditionOnlyIfAbsent,
	})
	require.NoError(t, err)
}

func TestClientCompleteMultipartUploadLockPeriod(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blockBlobAPI

	now := time.Now()
	expirationTime := now.AddDate(0, 0, 5)

	bAPI.
		EXPECT().
		CommitBlockList(
			gomock.Any(),
			gomock.Any(),
			immutabilityPolicyMatcher(blob.ImmutabilityPolicySettingLocked, expirationTime),
		).DoAndReturn(
		func(_ context.Context, base64BlockIDs []string, _ *blockblob.CommitBlockListOptions,
		) (blockblob.CommitBlockListResponse, error) {
			require.Equal(t, []string{"blob1", "blob2", "blob3"}, base64BlockIDs)

			return blockblob.CommitBlockListResponse{}, nil
		})

	parts := make([]objval.Part, 0)

	for i := 1; i <= 3; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("blob%d", i), Number: i})
	}

	err := client.CompleteMultipartUpload(context.Background(), objcli.CompleteMultipartUploadOptions{
		Bucket:   "container",
		UploadID: objcli.NoUploadID,
		Key:      "blob",
		Parts:    parts,
		Lock:     objcli.NewComplianceLock(expirationTime),
	})
	require.NoError(t, err)
}

func TestClientAbortMultipartUpload(t *testing.T) {
	client := &Client{}

	err := client.AbortMultipartUpload(context.Background(), objcli.AbortMultipartUploadOptions{
		Bucket:   "container",
		UploadID: objcli.NoUploadID,
		Key:      "blob",
	})
	require.NoError(t, err)
}

func TestClientAbortMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.AbortMultipartUpload(context.Background(), objcli.AbortMultipartUploadOptions{
		Bucket:   "container",
		UploadID: "id",
		Key:      "blob",
	})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientGetBucketLockingStatus(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	cAPI := testEnvironment.containerAPI

	output := container.GetPropertiesResponse{}

	output.IsImmutableStorageWithVersioningEnabled = ptr.To(true)

	cAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	object, err := client.GetBucketLockingStatus(context.Background(), objcli.GetBucketLockingStatusOptions{
		Bucket: "container",
	})
	require.NoError(t, err)

	expected := &objval.BucketLockingStatus{
		Enabled: true,
	}

	require.Equal(t, expected, object)
}

func TestClientSetObjectLock(t *testing.T) {
	testEnvironment := newTestEnvironment(t)
	client := testEnvironment.client
	bAPI := testEnvironment.blobAPI

	now := time.Now()
	expirationTime := now.AddDate(0, 0, 5)

	bAPI.
		EXPECT().
		SetImmutabilityPolicy(
			gomock.Any(),
			expirationTime,
			immutabilityModeMatcher(blob.ImmutabilityPolicySettingLocked),
		).Return(blob.SetImmutabilityPolicyResponse{}, nil)

	err := client.SetObjectLock(context.Background(), objcli.SetObjectLockOptions{
		Bucket: "container",
		Key:    "blob",
		Lock:   objcli.NewComplianceLock(expirationTime),
	})
	require.NoError(t, err)
}
