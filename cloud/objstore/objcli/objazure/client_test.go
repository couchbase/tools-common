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
	"github.com/aws/aws-sdk-go/aws"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/objstore/objval"
	"github.com/couchbase/tools-common/types/ptr"
)

func TestNewClient(t *testing.T) {
	require.Equal(t, &Client{serviceAPI: &serviceClient{}}, NewClient(ClientOptions{}))
}

func TestClientProvider(t *testing.T) {
	require.Equal(t, objval.ProviderAzure, (&Client{}).Provider())
}

func newTestClient(t *testing.T) (*Client, *MockcontainerAPI, *MockblockBlobAPI) {
	var (
		ctrl = gomock.NewController(t)
		sAPI = NewMockserviceAPI(ctrl)
		cAPI = NewMockcontainerAPI(ctrl)
		bAPI = NewMockblockBlobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient("container").Return(cAPI).AnyTimes()
	cAPI.EXPECT().NewBlockBlobClient("blob").Return(bAPI).AnyTimes()

	return &Client{serviceAPI: sAPI}, cAPI, bAPI
}

func TestClientGetObject(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	output := blob.DownloadStreamResponse{}

	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = aws.Int64(42)
	output.Body = io.NopCloser(strings.NewReader("value"))

	bAPI.
		EXPECT().
		DownloadStream(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, opts *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			require.Equal(t, int64(0), opts.Range.Count)
			require.Equal(t, int64(0), opts.Range.Offset)

			return output, nil
		})

	object, err := client.GetObject(context.Background(), "container", "blob", nil)
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         ptr.To[int64](42),
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)
}

func TestClientGetObjectWithByteRange(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	output := blob.DownloadStreamResponse{}

	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))
	output.ContentLength = aws.Int64(42)
	output.Body = io.NopCloser(strings.NewReader("value"))

	bAPI.
		EXPECT().
		DownloadStream(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, opts *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			require.Equal(t, int64(65), opts.Range.Count)
			require.Equal(t, int64(64), opts.Range.Offset)

			return output, nil
		})

	object, err := client.GetObject(context.Background(), "container", "blob", &objval.ByteRange{Start: 64, End: 128})
	require.NoError(t, err)

	expected := &objval.Object{
		ObjectAttrs: objval.ObjectAttrs{
			Key:          "blob",
			Size:         ptr.To[int64](42),
			LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
		},
		Body: io.NopCloser(strings.NewReader("value")),
	}

	require.Equal(t, expected, object)
}

func TestClientGetObjectWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.GetObject(context.Background(), "bucket", "blob", &objval.ByteRange{Start: 128, End: 64})

	var invalidByteRange *objval.InvalidByteRangeError

	require.ErrorAs(t, err, &invalidByteRange)
}

func TestClientGetObjectAttrs(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	output := blob.GetPropertiesResponse{}

	output.ContentLength = aws.Int64(42)
	output.ETag = ptr.To(azcore.ETag("etag"))
	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))

	bAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	attrs, err := client.GetObjectAttrs(context.Background(), "container", "blob")
	require.NoError(t, err)

	expected := &objval.ObjectAttrs{
		Key:          "blob",
		ETag:         ptr.To("etag"),
		Size:         ptr.To[int64](42),
		LastModified: aws.Time((time.Time{}).Add(24 * time.Hour)),
	}

	require.Equal(t, expected, attrs)
}

func TestClientPutObject(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	output := blockblob.UploadResponse{}

	fn := func(
		ctx context.Context, body io.ReadSeekCloser, opts *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error) {
		b := md5.Sum([]byte("value"))
		require.Equal(t, b[:], opts.TransactionalContentMD5)

		return output, nil
	}

	bAPI.
		EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(fn)

	require.NoError(t, client.PutObject(context.Background(), "container", "blob", strings.NewReader("value")))
}

func TestClientAppendToObjectNotExists(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	bAPI.
		EXPECT().
		GetProperties(gomock.Any(), gomock.Any()).
		Return(blob.GetPropertiesResponse{}, &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)})

	output := blockblob.UploadResponse{}

	fn := func(
		ctx context.Context, body io.ReadSeekCloser, opts *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error) {
		b := md5.Sum([]byte("value"))
		require.Equal(t, b[:], opts.TransactionalContentMD5)

		return output, nil
	}

	bAPI.
		EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(fn)

	require.NoError(t, client.AppendToObject(context.Background(), "container", "blob", strings.NewReader("value")))
}

func TestClientAppendToObject(t *testing.T) {
	client, cAPI, bAPI := newTestClient(t)

	output := blob.GetPropertiesResponse{}

	output.ContentLength = aws.Int64(42)
	output.ETag = ptr.To(azcore.ETag("etag"))
	output.LastModified = aws.Time((time.Time{}).Add(24 * time.Hour))

	blobClient := NewMockblobAPI(cAPI.ctrl)
	cAPI.EXPECT().NewBlobClient(gomock.Any()).Return(blobClient)
	blobClient.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("example.com", nil)

	bAPI.EXPECT().GetProperties(gomock.Any(), gomock.Any()).Return(output, nil)

	bAPI.
		EXPECT().
		StageBlockFromURL(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context, base64BlockID, sourceURL string, options *blockblob.StageBlockFromURLOptions,
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
			ctx context.Context, base64BlockIDs []string, options *blockblob.CommitBlockListOptions,
		) (blockblob.CommitBlockListResponse, error) {
			require.Len(t, base64BlockIDs, 2)
			return blockblob.CommitBlockListResponse{}, nil
		})

	require.NoError(t, client.AppendToObject(context.Background(), "container", "blob", strings.NewReader("value")))
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

	require.NoError(t, client.DeleteObjects(context.Background(), "container", "blob1", "blob2"))
}

func TestClientDeleteObjectsKeyNotFound(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	bAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(
		blob.DeleteResponse{},
		&azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)},
	)

	require.NoError(t, client.DeleteObjects(context.Background(), "container", "blob"))
}

func TestClientCreateMultipartUpload(t *testing.T) {
	client := &Client{}

	id, err := client.CreateMultipartUpload(context.Background(), "container", "blob")
	require.NoError(t, err)
	require.Equal(t, objcli.NoUploadID, id)
}

func TestClientUploadPartWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPart(context.Background(), "container", "id", "blob", 42, nil)
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientListParts(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	output := blockblob.GetBlockListResponse{}
	output.BlockList = blockblob.BlockList{
		UncommittedBlocks: []*blockblob.Block{
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

	bAPI.EXPECT().GetBlockList(gomock.Any(), gomock.Any(), gomock.Any()).Return(output, nil)

	parts, err := client.ListParts(context.Background(), "container", objcli.NoUploadID, "blob")
	require.NoError(t, err)

	expected := []objval.Part{{ID: "block3", Size: 256}, {ID: "block4", Size: 512}}
	require.Equal(t, expected, parts)
}

func TestClientListPartsWithUploadID(t *testing.T) {
	client := &Client{}

	_, err := client.ListParts(context.Background(), "container", "id", "blob")
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientUploadPart(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	bAPI.
		EXPECT().
		StageBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context, base64BlockID string, body io.ReadSeekCloser, options *blockblob.StageBlockOptions,
		) (blockblob.StageBlockResponse, error) {
			return blockblob.StageBlockResponse{}, nil
		})

	part, err := client.UploadPart(context.Background(), "container", objcli.NoUploadID, "blob", 42,
		strings.NewReader("value"))
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

			client := Client{serviceAPI: sAPI}

			srcAPI.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("example.com", nil)

			dstAPI.
				EXPECT().
				StageBlockFromURL(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					ctx context.Context, base64BlockID, url string, options *blockblob.StageBlockFromURLOptions,
				) (blockblob.StageBlockFromURLResponse, error) {
					require.Equal(t, test.eOffset, options.Range.Offset)
					require.Equal(t, test.eLength, options.Range.Count)

					_, err := base64.StdEncoding.DecodeString(base64BlockID)
					require.NoError(t, err)

					return blockblob.StageBlockFromURLResponse{}, nil
				})

			part, err := client.UploadPartCopy(context.Background(), "container", objcli.NoUploadID, "dst", "src", 42,
				test.br)
			require.NoError(t, err)
			require.NotZero(t, part.ID)

			_, err = base64.StdEncoding.DecodeString(part.ID)
			require.NoError(t, err)
		})
	}
}

func TestClientUploadPartCopy(t *testing.T) {
	var (
		ctrl       = gomock.NewController(t)
		sAPI       = NewMockserviceAPI(ctrl)
		cAPI       = NewMockcontainerAPI(ctrl)
		srcAPI     = NewMockblockBlobAPI(ctrl)
		srcBlobAPI = NewMockblobAPI(ctrl)
		dstAPI     = NewMockblockBlobAPI(ctrl)
	)

	sAPI.EXPECT().NewContainerClient("container").Return(cAPI).Times(3)
	cAPI.EXPECT().NewBlockBlobClient("src").Return(srcAPI)
	cAPI.EXPECT().NewBlockBlobClient("dst").Return(dstAPI)
	cAPI.EXPECT().NewBlobClient("src").Return(srcBlobAPI)

	srcBlobAPI.EXPECT().GetSASURL(gomock.Any(), gomock.Any(), gomock.Any()).Return("", errors.New(sasErrString))
	srcAPI.EXPECT().URL().Return("example.com")

	client := Client{serviceAPI: sAPI}

	dstAPI.
		EXPECT().
		StageBlockFromURL(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context, base64BlockID, url string, options *blockblob.StageBlockFromURLOptions,
		) (blockblob.StageBlockFromURLResponse, error) {
			require.Equal(t, int64(0), options.Range.Count)

			_, err := base64.StdEncoding.DecodeString(base64BlockID)
			require.NoError(t, err)

			return blockblob.StageBlockFromURLResponse{}, nil
		})

	part, err := client.UploadPartCopy(context.Background(), "container", objcli.NoUploadID, "dst", "src", 42, nil)
	require.NoError(t, err)
	require.NotZero(t, part.ID)

	_, err = base64.StdEncoding.DecodeString(part.ID)
	require.NoError(t, err)
}

func TestClientUploadPartCopyWithInvalidByteRange(t *testing.T) {
	client := &Client{}

	_, err := client.UploadPartCopy(context.Background(),
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

	_, err := client.UploadPartCopy(context.Background(), "bucket", "id", "dst", "src", 42, nil)
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.CompleteMultipartUpload(context.Background(), "bucket", "id", "blob", objval.Part{})
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}

func TestClientCompleteMultipartUploadOverMaxComposable(t *testing.T) {
	client, _, bAPI := newTestClient(t)

	bAPI.
		EXPECT().
		CommitBlockList(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context, base64BlockIDs []string, options *blockblob.CommitBlockListOptions,
		) (blockblob.CommitBlockListResponse, error) {
			require.Equal(t, []string{"blob1", "blob2", "blob3"}, base64BlockIDs)

			return blockblob.CommitBlockListResponse{}, nil
		})

	parts := make([]objval.Part, 0)

	for i := 1; i <= 3; i++ {
		parts = append(parts, objval.Part{ID: fmt.Sprintf("blob%d", i), Number: i})
	}

	require.NoError(
		t,
		client.CompleteMultipartUpload(context.Background(), "container", objcli.NoUploadID, "blob", parts...),
	)
}

func TestClientAbortMultipartUpload(t *testing.T) {
	client := &Client{}

	require.NoError(t, client.AbortMultipartUpload(context.Background(), "container", objcli.NoUploadID, "blob"))
}

func TestClientAbortMultipartUploadWithUploadID(t *testing.T) {
	client := &Client{}

	err := client.AbortMultipartUpload(context.Background(), "container", "id", "blob")
	require.ErrorIs(t, err, objcli.ErrExpectedNoUploadID)
}
