package objazure

import (
	"context"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

//go:generate go run github.com/golang/mock/mockgen -source ./api.go -destination ./mock_api.go -package objazure
type serviceAPI interface {
	NewContainerClient(containerName string) containerAPI
}

type serviceClient struct {
	client *service.Client
}

var _ serviceAPI = (*serviceClient)(nil)

func (c *serviceClient) NewContainerClient(containerName string) containerAPI {
	return containerClient{c.client.NewContainerClient(containerName)}
}

type containerAPI interface {
	NewBlobClient(blobName string) blobAPI
	NewBlockBlobClient(blobName string) blockBlobAPI
	NewListBlobsFlatPager(o *container.ListBlobsFlatOptions) flatBlobsPager
	NewListBlobsHierarchyPager(delimiter string, o *container.ListBlobsHierarchyOptions) hierarchyBlobsPager
}

type containerClient struct {
	client *container.Client
}

func (c containerClient) NewBlockBlobClient(blobName string) blockBlobAPI {
	return c.client.NewBlockBlobClient(blobName)
}

func (c containerClient) NewBlobClient(blobName string) blobAPI {
	return c.client.NewBlobClient(blobName)
}

func (c containerClient) NewListBlobsFlatPager(o *container.ListBlobsFlatOptions) flatBlobsPager {
	return c.client.NewListBlobsFlatPager(o)
}

func (c containerClient) NewListBlobsHierarchyPager(
	delimiter string, o *container.ListBlobsHierarchyOptions,
) hierarchyBlobsPager {
	return c.client.NewListBlobsHierarchyPager(delimiter, o)
}

type blobAPI interface {
	GetSASURL(permissions sas.BlobPermissions, start, expiry time.Time) (string, error)
}

var _ blobAPI = (*blob.Client)(nil)

type flatBlobsPager interface {
	More() bool
	NextPage(ctx context.Context) (azblob.ListBlobsFlatResponse, error)
}

type hierarchyBlobsPager interface {
	More() bool
	NextPage(ctx context.Context) (container.ListBlobsHierarchyResponse, error)
}

// blockBlobAPI is a block blob interface which allows interactions with a block blob stored in an Azure container.
type blockBlobAPI interface {
	Delete(ctx context.Context, options *blob.DeleteOptions) (blob.DeleteResponse, error)
	DownloadStream(ctx context.Context, o *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error)
	GetProperties(ctx context.Context, options *blob.GetPropertiesOptions) (blob.GetPropertiesResponse,
		error)
	CommitBlockList(ctx context.Context, base64BlockIDs []string, options *blockblob.CommitBlockListOptions,
	) (blockblob.CommitBlockListResponse, error)
	GetBlockList(ctx context.Context, listType blockblob.BlockListType, options *blockblob.GetBlockListOptions,
	) (blockblob.GetBlockListResponse, error)
	StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeekCloser, options *blockblob.StageBlockOptions,
	) (blockblob.StageBlockResponse, error)
	StageBlockFromURL(ctx context.Context, base64BlockID, sourceURL string, length int64,
		options *blockblob.StageBlockFromURLOptions) (blockblob.StageBlockFromURLResponse, error)
	URL() string
	Upload(ctx context.Context, body io.ReadSeekCloser, options *blockblob.UploadOptions,
	) (blockblob.UploadResponse, error)
}

var _ blockBlobAPI = (*blockblob.Client)(nil)
