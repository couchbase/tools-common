package objazure

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

//go:generate mockery --all --case underscore --inpackage
type readSeekNoopCloser struct {
	io.ReadSeeker
}

func (r readSeekNoopCloser) Close() error { return nil }

// blobStorageAPI is a top level interface which allows interactions with the Azure blob storage service.
type blobStorageAPI interface {
	ToContainerAPI(container string) (containerAPI, error)
	ToBlobAPI(container, blob string) (blobAPI, error)
}

var _ blobStorageAPI = (*serviceClient)(nil)

// serviceClient implements the 'blobStorageAPI' interface and encapsulates the Azure SDK in a unit testable interface.
type serviceClient struct {
	client *azblob.ServiceClient
}

func (s serviceClient) ToContainerAPI(container string) (containerAPI, error) {
	client, err := s.client.NewContainerClient(container)
	if err != nil {
		return nil, err
	}

	return containerClient{client: client}, nil
}

func (s serviceClient) ToBlobAPI(container, blob string) (blobAPI, error) {
	containerClient, err := s.ToContainerAPI(container)
	if err != nil {
		return nil, err
	}

	blobClient, err := containerClient.ToBlobAPI(blob)
	if err != nil {
		return nil, err
	}

	return blobClient, nil
}

// containerAPI is a container level interface which allows interactions with an Azure blob storage container.
type containerAPI interface {
	GetListBlobsFlatPagerAPI(options azblob.ContainerListBlobsFlatOptions) listBlobsPagerAPI
	GetListBlobsHierarchyPagerAPI(delimiter string, options azblob.ContainerListBlobsHierarchyOptions) listBlobsPagerAPI
	ToBlobAPI(blob string) (blobAPI, error)
}

var _ containerAPI = (*containerClient)(nil)

// containerClient implements the 'containerAPI' interface and encapsulates the Azure SDK in a unit testable interface.
type containerClient struct {
	client *azblob.ContainerClient
}

type listBlobsPagerAPI interface {
	GetNextListBlobsSegment(ctx context.Context) ([]*azblob.BlobPrefix, []*azblob.BlobItemInternal, error)
}

type commonAzurePager interface {
	NextPage(ctx context.Context) bool
	Err() error
}

type basePager struct {
	pager commonAzurePager
}

func (b basePager) nextPage(ctx context.Context) error {
	var (
		hasNextPage = b.pager.NextPage(ctx)
		err         = b.pager.Err()
	)

	if err != nil {
		return err
	}

	if !hasNextPage {
		return errPagerNoMorePages
	}

	return nil
}

type flatPager struct {
	basePager
	pager *azblob.ContainerListBlobFlatPager
}

func (f flatPager) GetNextListBlobsSegment(ctx context.Context) ([]*azblob.BlobPrefix,
	[]*azblob.BlobItemInternal, error,
) {
	err := f.nextPage(ctx)
	if err != nil {
		return nil, nil, err
	}

	return nil, f.pager.PageResponse().Segment.BlobItems, nil
}

type hierarchyPager struct {
	basePager
	pager *azblob.ContainerListBlobHierarchyPager
}

func (f hierarchyPager) GetNextListBlobsSegment(ctx context.Context) ([]*azblob.BlobPrefix,
	[]*azblob.BlobItemInternal, error,
) {
	err := f.nextPage(ctx)
	if err != nil {
		return nil, nil, err
	}

	segment := f.pager.PageResponse().Segment

	return segment.BlobPrefixes, segment.BlobItems, nil
}

func (c containerClient) GetListBlobsFlatPagerAPI(options azblob.ContainerListBlobsFlatOptions) listBlobsPagerAPI {
	pager := c.client.ListBlobsFlat(&options)

	return flatPager{basePager: basePager{pager: pager}, pager: pager}
}

func (c containerClient) GetListBlobsHierarchyPagerAPI(delimiter string,
	options azblob.ContainerListBlobsHierarchyOptions,
) listBlobsPagerAPI {
	pager := c.client.ListBlobsHierarchy(delimiter, &options)

	return hierarchyPager{basePager: basePager{pager: pager}, pager: pager}
}

func (c containerClient) ToBlobAPI(blob string) (blobAPI, error) {
	client, err := c.client.NewBlockBlobClient(blob)
	if err != nil {
		return nil, err
	}

	return blobClient{client: client}, nil
}

// blobAPI is a block blob interface which allows interactions with a block blob stored in an Azure container.
type blobAPI interface {
	Delete(ctx context.Context, options azblob.BlobDeleteOptions) (azblob.BlobDeleteResponse, error)
	Download(ctx context.Context, options azblob.BlobDownloadOptions) (azblob.BlobDownloadResponse, error)
	GetProperties(ctx context.Context, options azblob.BlobGetPropertiesOptions) (azblob.BlobGetPropertiesResponse,
		error)
	CommitBlockList(ctx context.Context, base64BlockIDs []string, options azblob.BlockBlobCommitBlockListOptions,
	) (azblob.BlockBlobCommitBlockListResponse, error)
	GetBlockList(ctx context.Context, listType azblob.BlockListType, options azblob.BlockBlobGetBlockListOptions,
	) (azblob.BlockBlobGetBlockListResponse, error)
	StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeeker, options azblob.BlockBlobStageBlockOptions,
	) (azblob.BlockBlobStageBlockResponse, error)
	StageBlockFromURL(ctx context.Context, base64BlockID, sourceURL string, length int64,
		options azblob.BlockBlobStageBlockFromURLOptions) (azblob.BlockBlobStageBlockFromURLResponse, error)
	URL() string
	Upload(ctx context.Context, body io.ReadSeeker, options azblob.BlockBlobUploadOptions,
	) (azblob.BlockBlobUploadResponse, error)
}

var _ blobAPI = (*blobClient)(nil)

// blobClient implements the 'blobAPI' interface and encapsulates the Azure SDK in a unit testable interface.
type blobClient struct {
	client *azblob.BlockBlobClient
}

func (b blobClient) Delete(ctx context.Context, options azblob.BlobDeleteOptions,
) (azblob.BlobDeleteResponse, error) {
	return b.client.Delete(ctx, &options)
}

func (b blobClient) Download(ctx context.Context, options azblob.BlobDownloadOptions,
) (azblob.BlobDownloadResponse, error) {
	return b.client.Download(ctx, &options)
}

func (b blobClient) GetProperties(ctx context.Context, options azblob.BlobGetPropertiesOptions,
) (azblob.BlobGetPropertiesResponse, error) {
	return b.client.GetProperties(ctx, &options)
}

func (b blobClient) CommitBlockList(ctx context.Context, base64BlockIDs []string,
	options azblob.BlockBlobCommitBlockListOptions,
) (azblob.BlockBlobCommitBlockListResponse, error) {
	return b.client.CommitBlockList(ctx, base64BlockIDs, &options)
}

func (b blobClient) GetBlockList(ctx context.Context, listType azblob.BlockListType,
	options azblob.BlockBlobGetBlockListOptions,
) (azblob.BlockBlobGetBlockListResponse, error) {
	return b.client.GetBlockList(ctx, listType, &options)
}

func (b blobClient) StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeeker,
	options azblob.BlockBlobStageBlockOptions,
) (azblob.BlockBlobStageBlockResponse, error) {
	return b.client.StageBlock(ctx, base64BlockID, readSeekNoopCloser{body}, &options)
}

func (b blobClient) StageBlockFromURL(ctx context.Context, base64BlockID, sourceURL string, length int64,
	options azblob.BlockBlobStageBlockFromURLOptions,
) (azblob.BlockBlobStageBlockFromURLResponse, error) {
	return b.client.StageBlockFromURL(ctx, base64BlockID, sourceURL, length, &options)
}

func (b blobClient) URL() string {
	return b.client.URL()
}

func (b blobClient) Upload(ctx context.Context, body io.ReadSeeker, options azblob.BlockBlobUploadOptions,
) (azblob.BlockBlobUploadResponse, error) {
	return b.client.Upload(ctx, readSeekNoopCloser{body}, &options)
}
