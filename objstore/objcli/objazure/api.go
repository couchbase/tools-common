package objazure

import (
	"context"
	"io"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

//go:generate mockery --all --case underscore --inpackage

// blobStorageAPI is a top level interface which allows interactions with the Azure blob storage service.
type blobStorageAPI interface {
	ToContainerAPI(container string) containerAPI
}

// serviceURL implements the 'blobStorageAPI' interface and encapsulates the Azure SDK in a unit
// testable interface.
type serviceURL struct {
	url azblob.ServiceURL
}

func (s serviceURL) ToContainerAPI(container string) containerAPI {
	url := s.url.NewContainerURL(container)
	return containerURL{url: url}
}

// containerAPI is a container level interface which allows interactions with an Azure blob storage container.
type containerAPI interface {
	ListBlobsFlatSegment(ctx context.Context, marker azblob.Marker,
		o azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsFlatSegmentResponse, error)
	ListBlobsHierarchySegment(ctx context.Context, marker azblob.Marker, delimiter string,
		o azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsHierarchySegmentResponse, error)
	ToBlobAPI(blob string) blobAPI
}

// containerURL implements the 'containerAPI' interface and encapsulates the Azure SDK in a unit testable interface.
type containerURL struct {
	url azblob.ContainerURL
}

func (c containerURL) ListBlobsFlatSegment(ctx context.Context, marker azblob.Marker,
	o azblob.ListBlobsSegmentOptions,
) (*azblob.ListBlobsFlatSegmentResponse, error) {
	return c.url.ListBlobsFlatSegment(ctx, marker, o)
}

func (c containerURL) ListBlobsHierarchySegment(ctx context.Context, marker azblob.Marker, delimiter string,
	o azblob.ListBlobsSegmentOptions,
) (*azblob.ListBlobsHierarchySegmentResponse, error) {
	return c.url.ListBlobsHierarchySegment(ctx, marker, delimiter, o)
}

func (c containerURL) ToBlobAPI(blob string) blobAPI {
	url := c.url.NewBlobURL(blob)
	return blobURL{url: url}
}

// blobAPI is a blob level interface which allows interactions with a blob stored in an Azure container.
type blobAPI interface {
	Delete(ctx context.Context, deleteOptions azblob.DeleteSnapshotsOptionType,
		ac azblob.BlobAccessConditions) (*azblob.BlobDeleteResponse, error)
	Download(ctx context.Context, offset, count int64, ac azblob.BlobAccessConditions,
		rangeGetContentMD5 bool, cpk azblob.ClientProvidedKeyOptions) (*azblob.DownloadResponse, error)
	GetProperties(ctx context.Context, ac azblob.BlobAccessConditions,
		cpk azblob.ClientProvidedKeyOptions) (*azblob.BlobGetPropertiesResponse, error)
	ToBlockBlobAPI() blockBlobAPI
}

// blobURL implements the 'blobAPI' interface and encapsulates the Azure SDK in a unit testable interface.
type blobURL struct {
	url azblob.BlobURL
}

func (b blobURL) ToBlockBlobAPI() blockBlobAPI {
	url := b.url.ToBlockBlobURL()
	return blockBlobURL{url: url}
}

func (b blobURL) Delete(ctx context.Context, deleteOptions azblob.DeleteSnapshotsOptionType,
	ac azblob.BlobAccessConditions,
) (*azblob.BlobDeleteResponse, error) {
	return b.url.Delete(ctx, deleteOptions, ac)
}

func (b blobURL) Download(ctx context.Context, offset, count int64, ac azblob.BlobAccessConditions,
	rangeGetContentMD5 bool, cpk azblob.ClientProvidedKeyOptions,
) (*azblob.DownloadResponse, error) {
	return b.url.Download(ctx, offset, count, ac, rangeGetContentMD5, cpk)
}

func (b blobURL) GetProperties(ctx context.Context,
	ac azblob.BlobAccessConditions, cpk azblob.ClientProvidedKeyOptions,
) (*azblob.BlobGetPropertiesResponse, error) {
	return b.url.GetProperties(ctx, ac, cpk)
}

// blockBlobAPI is a block blob interface which allows interactions with a block blob stored in an Azure container.
type blockBlobAPI interface {
	CommitBlockList(ctx context.Context, base64BlockIDs []string, h azblob.BlobHTTPHeaders, metadata azblob.Metadata,
		ac azblob.BlobAccessConditions, tier azblob.AccessTierType, blobTagsMap azblob.BlobTagsMap,
		cpk azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobCommitBlockListResponse, error)
	GetBlockList(ctx context.Context, listType azblob.BlockListType,
		ac azblob.LeaseAccessConditions) (*azblob.BlockList, error)
	StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeeker, ac azblob.LeaseAccessConditions,
		transactionalMD5 []byte, cpk azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobStageBlockResponse, error)
	StageBlockFromURL(ctx context.Context, base64BlockID string, sourceURL url.URL, offset, count int64,
		destinationAccessConditions azblob.LeaseAccessConditions,
		sourceAccessConditions azblob.ModifiedAccessConditions,
		cpk azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobStageBlockFromURLResponse, error)
	URL() url.URL
	Upload(ctx context.Context, body io.ReadSeeker, h azblob.BlobHTTPHeaders, metadata azblob.Metadata,
		ac azblob.BlobAccessConditions, tier azblob.AccessTierType, blobTagsMap azblob.BlobTagsMap,
		cpk azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobUploadResponse, error)
}

// blockBlobURL implements the 'blockBlobAPI' interface and encapsulates the Azure SDK in a unit testable interface.
type blockBlobURL struct {
	url azblob.BlockBlobURL
}

func (b blockBlobURL) CommitBlockList(
	ctx context.Context, base64BlockIDs []string, h azblob.BlobHTTPHeaders, metadata azblob.Metadata,
	ac azblob.BlobAccessConditions, tier azblob.AccessTierType, blobTagsMap azblob.BlobTagsMap,
	cpk azblob.ClientProvidedKeyOptions,
) (*azblob.BlockBlobCommitBlockListResponse, error) {
	return b.url.CommitBlockList(ctx, base64BlockIDs, h, metadata, ac, tier, blobTagsMap, cpk)
}

func (b blockBlobURL) GetBlockList(ctx context.Context, listType azblob.BlockListType,
	ac azblob.LeaseAccessConditions,
) (*azblob.BlockList, error) {
	return b.url.GetBlockList(ctx, listType, ac)
}

func (b blockBlobURL) StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeeker,
	ac azblob.LeaseAccessConditions, transactionalMD5 []byte,
	cpk azblob.ClientProvidedKeyOptions,
) (*azblob.BlockBlobStageBlockResponse, error) {
	return b.url.StageBlock(ctx, base64BlockID, body, ac, transactionalMD5, cpk)
}

func (b blockBlobURL) StageBlockFromURL(ctx context.Context, base64BlockID string, sourceURL url.URL,
	offset, count int64, destinationAccessConditions azblob.LeaseAccessConditions,
	sourceAccessConditions azblob.ModifiedAccessConditions,
	cpk azblob.ClientProvidedKeyOptions,
) (*azblob.BlockBlobStageBlockFromURLResponse, error) {
	return b.url.StageBlockFromURL(ctx, base64BlockID, sourceURL, offset, count, destinationAccessConditions,
		sourceAccessConditions, cpk)
}

func (b blockBlobURL) URL() url.URL {
	return b.url.URL()
}

func (b blockBlobURL) Upload(ctx context.Context, body io.ReadSeeker, h azblob.BlobHTTPHeaders,
	metadata azblob.Metadata, ac azblob.BlobAccessConditions, tier azblob.AccessTierType,
	blobTagsMap azblob.BlobTagsMap, cpk azblob.ClientProvidedKeyOptions,
) (*azblob.BlockBlobUploadResponse, error) {
	return b.url.Upload(ctx, body, h, metadata, ac, tier, blobTagsMap, cpk)
}
