package objazure

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"

	"github.com/couchbase/tools-common/hofp"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/system"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// NOTE: As apposed to AWS/GCP, Azure use the container/blob naming convention, however, for consistency the Azure
// client implementation continues to use the bucket/key names.

// Client implements the 'objcli.Client' interface allowing the creation/management of blobs stored in Azure blob store.
type Client struct {
	storageAPI blobStorageAPI
}

var _ objcli.Client = (*Client)(nil)

// ClientOptions encapsulates the options for creating a new Azure Client.
type ClientOptions struct {
	// Client represents a URL to the Azure Blob Storage service allowing you to manipulate blob containers.
	//
	// NOTE: Required
	Client *azblob.ServiceClient
}

// NewClient returns a new client which uses the given service client, in general this should be the one created using
// the 'azblob.NewServiceClient' function exposed by the SDK.
func NewClient(options ClientOptions) *Client {
	return &Client{storageAPI: serviceClient{client: options.Client}}
}

func (c *Client) Provider() objval.Provider {
	return objval.ProviderAzure
}

func (c *Client) GetObject(ctx context.Context, bucket, key string, br *objval.ByteRange) (*objval.Object, error) {
	if err := br.Valid(false); err != nil {
		return nil, err // Purposefully not wrapped
	}

	var offset, length int64 = 0, azblob.CountToEnd
	if br != nil {
		offset, length = br.ToOffsetLength(length)
	}

	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	resp, err := blobClient.Download(ctx, azblob.BlobDownloadOptions{Offset: &offset, Count: &length})
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	attrs := objval.ObjectAttrs{
		Key:          key,
		Size:         *resp.ContentLength,
		LastModified: resp.LastModified,
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        resp.Body(&azblob.RetryReaderOptions{}),
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(ctx context.Context, bucket, key string) (*objval.ObjectAttrs, error) {
	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	resp, err := blobClient.GetProperties(ctx, azblob.BlobGetPropertiesOptions{})
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          key,
		ETag:         *resp.ETag,
		Size:         *resp.ContentLength,
		LastModified: resp.LastModified,
	}

	return attrs, nil
}

func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.ReadSeeker) error {
	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return handleError(bucket, key, err)
	}

	md5sum := md5.New()

	_, err = aws.CopySeekableBody(io.MultiWriter(md5sum), body)
	if err != nil {
		return fmt.Errorf("failed to calculate checksums: %w", err)
	}

	_, err = blobClient.Upload(
		ctx,
		body,
		azblob.BlockBlobUploadOptions{TransactionalContentMD5: md5sum.Sum(nil)},
	)

	return handleError(bucket, key, err)
}

func (c *Client) AppendToObject(ctx context.Context, bucket, key string, data io.ReadSeeker) error {
	attrs, err := c.GetObjectAttrs(ctx, bucket, key)

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) || attrs != nil && attrs.Size == 0 {
		return c.PutObject(ctx, bucket, key, data)
	}

	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
	}

	id, err := c.CreateMultipartUpload(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("failed to start multipart upload: %w", err)
	}

	existing, err := c.UploadPartCopy(ctx, bucket, id, key, key, objcli.NoPartNumber, nil)
	if err != nil {
		return fmt.Errorf("failed to get existing object part: %w", err)
	}

	intermediate, err := c.UploadPart(ctx, bucket, id, key, objcli.NoPartNumber, data)
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	err = c.CompleteMultipartUpload(ctx, bucket, id, key, existing, intermediate)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (c *Client) DeleteObjects(ctx context.Context, bucket string, keys ...string) error {
	containerClient, err := c.storageAPI.ToContainerAPI(bucket)
	if err != nil {
		return err // Purposefully not wrapped
	}

	pool := hofp.NewPool(hofp.Options{
		Context:   ctx,
		Size:      system.NumWorkers(len(keys)),
		LogPrefix: "(objazure)",
	})

	del := func(ctx context.Context, key string) error {
		blobClient, err := containerClient.ToBlobAPI(key)
		if err != nil {
			return err // Purposefully not wrapped
		}

		_, err = blobClient.Delete(ctx, azblob.BlobDeleteOptions{})
		if err != nil && !isKeyNotFound(err) {
			return handleError(bucket, key, err)
		}

		return nil
	}

	queue := func(key string) error {
		return pool.Queue(func(ctx context.Context) error { return del(ctx, key) })
	}

	for _, key := range keys {
		if queue(key) != nil {
			break
		}
	}

	return pool.Stop()
}

func (c *Client) DeleteDirectory(ctx context.Context, bucket, prefix string) error {
	fn := func(attrs *objval.ObjectAttrs) error {
		return c.DeleteObjects(ctx, bucket, attrs.Key)
	}

	return c.IterateObjects(ctx, bucket, prefix, "", nil, nil, fn)
}

func (c *Client) IterateObjects(ctx context.Context, bucket, prefix, delimiter string, include,
	exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	if include != nil && exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	containerClient, err := c.storageAPI.ToContainerAPI(bucket)
	if err != nil {
		return err // Purposefully not wrapped
	}

	if delimiter == "" {
		return c.iterateObjectsFlat(ctx, containerClient, bucket, prefix, include, exclude, fn)
	}

	return c.iterateObjectsHierarchy(ctx, containerClient, bucket, prefix, delimiter, include, exclude, fn)
}

func (c *Client) iterateObjectsFlat(ctx context.Context, containerClient containerAPI, bucket, prefix string, include,
	exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	options := azblob.ContainerListBlobsFlatOptions{Prefix: &prefix}

	return c.iterateObjectsWithPager(
		ctx,
		containerClient.GetListBlobsFlatPagerAPI(options),
		bucket,
		include,
		exclude,
		fn,
	)
}

func (c *Client) iterateObjectsHierarchy(ctx context.Context, containerClient containerAPI, bucket, prefix,
	delimiter string, include, exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	options := azblob.ContainerListBlobsHierarchyOptions{Prefix: &prefix}

	return c.iterateObjectsWithPager(
		ctx,
		containerClient.GetListBlobsHierarchyPagerAPI(delimiter, options),
		bucket,
		include,
		exclude,
		fn,
	)
}

func (c *Client) iterateObjectsWithPager(ctx context.Context, pager listBlobsPagerAPI, bucket string, include,
	exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	var (
		prefixes []*azblob.BlobPrefix
		blobs    []*azblob.BlobItemInternal
		objects  []*objval.ObjectAttrs
		err      error
	)

	for err == nil {
		prefixes, blobs, err = pager.GetNextListBlobsSegment(ctx)

		if errors.Is(err, errPagerNoMorePages) {
			break
		}

		if err != nil {
			return handleError(bucket, "", err)
		}

		objects = c.convertBlobsToObjectAttrs(prefixes, blobs)

		err = c.iterateObjects(objects, include, exclude, fn)
		if err != nil {
			return handleError(bucket, "", err)
		}
	}

	return nil
}

func (c *Client) convertBlobsToObjectAttrs(prefixes []*azblob.BlobPrefix, blobs []*azblob.BlobItemInternal,
) []*objval.ObjectAttrs {
	converted := make([]*objval.ObjectAttrs, 0, len(prefixes)+len(blobs))

	for _, p := range prefixes {
		converted = append(converted, &objval.ObjectAttrs{Key: *p.Name})
	}

	for _, b := range blobs {
		converted = append(converted, &objval.ObjectAttrs{
			Key:          *b.Name,
			Size:         *b.Properties.ContentLength,
			LastModified: b.Properties.LastModified,
		})
	}

	return converted
}

// iterateObjects iterates over the given segment (<=5000) of objects executing the given function for each object which
// has not been explicitly ignored by the user.
func (c *Client) iterateObjects(objects []*objval.ObjectAttrs, include, exclude []*regexp.Regexp,
	fn objcli.IterateFunc,
) error {
	for _, attrs := range objects {
		if objcli.ShouldIgnore(attrs.Key, include, exclude) {
			continue
		}

		// If the caller has returned an error, stop iteration, and return control to them
		if err := fn(attrs); err != nil {
			return err // Purposefully not wrapped
		}
	}

	return nil
}

func (c *Client) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	return objcli.NoUploadID, nil
}

func (c *Client) ListParts(ctx context.Context, bucket, id, key string) ([]objval.Part, error) {
	if id != objcli.NoUploadID {
		return nil, objcli.ErrExpectedNoUploadID
	}

	return c.listParts(ctx, bucket, key, azblob.BlockListTypeUncommitted)
}

func (c *Client) listParts(
	ctx context.Context, bucket, key string, blockType azblob.BlockListType,
) ([]objval.Part, error) {
	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	resp, err := blobClient.GetBlockList(
		ctx,
		blockType,
		azblob.BlockBlobGetBlockListOptions{},
	)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	// GetBlockList() will only return blocks of the required type/types so we can handle them in a general way
	parts := make([]objval.Part, 0, len(resp.CommittedBlocks)+len(resp.UncommittedBlocks))

	for _, block := range resp.CommittedBlocks {
		parts = append(parts, objval.Part{ID: *block.Name, Size: *block.Size})
	}

	for _, block := range resp.UncommittedBlocks {
		parts = append(parts, objval.Part{ID: *block.Name, Size: *block.Size})
	}

	return parts, nil
}

func (c *Client) UploadPart(
	ctx context.Context, bucket, id, key string, number int, body io.ReadSeeker,
) (objval.Part, error) {
	if id != objcli.NoUploadID {
		return objval.Part{}, objcli.ErrExpectedNoUploadID
	}

	size, err := aws.SeekerLen(body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to determine body length: %w", err)
	}

	var (
		md5sum  = md5.New()
		blockID = base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))
	)

	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return objval.Part{}, handleError(bucket, key, err)
	}

	_, err = aws.CopySeekableBody(md5sum, body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to calculate checksums: %w", err)
	}

	_, err = blobClient.StageBlock(
		ctx,
		blockID,
		body,
		azblob.BlockBlobStageBlockOptions{TransactionalContentMD5: md5sum.Sum(nil)},
	)

	return objval.Part{ID: blockID, Number: number, Size: size}, handleError(bucket, key, err)
}

func (c *Client) UploadPartCopy(
	ctx context.Context, bucket, id, dst, src string, number int, br *objval.ByteRange,
) (objval.Part, error) {
	if id != objcli.NoUploadID {
		return objval.Part{}, objcli.ErrExpectedNoUploadID
	}

	if err := br.Valid(false); err != nil {
		return objval.Part{}, err // Purposefully not wrapped
	}

	var offset, length int64 = 0, azblob.CountToEnd
	if br != nil {
		offset, length = br.ToOffsetLength(length)
	}

	blockID := base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))

	srcURL, err := c.getUploadPartCopySrcURL(bucket, src)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to get the source part URL: %w", err)
	}

	dstClient, err := c.storageAPI.ToBlobAPI(bucket, dst)
	if err != nil {
		return objval.Part{}, handleError(bucket, dst, err)
	}

	_, err = dstClient.StageBlockFromURL(
		ctx,
		blockID,
		srcURL,
		0, // Should be set to 0 (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url)
		azblob.BlockBlobStageBlockFromURLOptions{Offset: &offset, Count: &length},
	)
	if err != nil {
		return objval.Part{}, handleError(bucket, dst, err)
	}

	return objval.Part{ID: blockID, Number: number, Size: length}, nil
}

func (c *Client) getUploadPartCopySrcURL(bucket, src string) (string, error) {
	srcClient, err := c.storageAPI.ToBlobAPI(bucket, src)
	if err != nil {
		return "", handleError(bucket, src, err)
	}

	// We only need a SAS token when the service client is using shared key (static) credentials and, conveniently, the
	// SDK only allows creating SAS tokens if this type of credentials is used. Therefore, we know that if we cannot
	// create a SAS token, we don't need it and can return here.
	if !c.storageAPI.CanGetSASToken() {
		return srcClient.URL(), nil
	}

	srcBlobParts, err := azblob.NewBlobURLParts(srcClient.URL())
	if err != nil {
		return "", fmt.Errorf("failed to create new blob URL parts: %w", err)
	}

	var (
		permissions = azblob.BlobSASPermissions{Read: true}
		start       = time.Now().UTC()
		expiry      = start.Add(48 * time.Hour)
	)

	srcBlobParts.SAS, err = srcClient.GetSASToken(permissions, start, expiry)
	if err != nil {
		return "", fmt.Errorf("failed to get SAS token: %w", err)
	}

	return srcBlobParts.URL(), nil
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, bucket, id, key string, parts ...objval.Part) error {
	if id != objcli.NoUploadID {
		return objcli.ErrExpectedNoUploadID
	}

	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return handleError(bucket, key, err)
	}

	converted := make([]string, 0, len(parts))

	for _, part := range parts {
		converted = append(converted, part.ID)
	}

	_, err = blobClient.CommitBlockList(
		ctx,
		converted,
		azblob.BlockBlobCommitBlockListOptions{},
	)

	return handleError(bucket, key, err)
}

func (c *Client) AbortMultipartUpload(ctx context.Context, _, id, _ string) error {
	if id != objcli.NoUploadID {
		return objcli.ErrExpectedNoUploadID
	}

	// NOTE: Azure doesn't support removing/cleaning up staged blocks; it automatically garbage collects them after a
	// certain amount of time.

	return nil
}
