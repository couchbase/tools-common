package objazure

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"regexp"

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

// NewClient returns a new client which uses the given service client, in general this should be the one created using
// the 'azblob.NewServiceClient' function exposed by the SDK.
func NewClient(client *azblob.ServiceClient) *Client {
	return &Client{storageAPI: serviceClient{client: client}}
}

func (c *Client) Provider() objval.Provider {
	return objval.ProviderAzure
}

func (c *Client) GetObject(bucket, key string, br *objval.ByteRange) (*objval.Object, error) {
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

	resp, err := blobClient.Download(context.Background(), azblob.BlobDownloadOptions{Offset: &offset, Count: &length})
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

func (c *Client) GetObjectAttrs(bucket, key string) (*objval.ObjectAttrs, error) {
	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	resp, err := blobClient.GetProperties(context.Background(), azblob.BlobGetPropertiesOptions{})
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

func (c *Client) PutObject(bucket, key string, body io.ReadSeeker) error {
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
		context.Background(),
		body,
		azblob.BlockBlobUploadOptions{TransactionalContentMD5: md5sum.Sum(nil)},
	)

	return handleError(bucket, key, err)
}

func (c *Client) AppendToObject(bucket, key string, data io.ReadSeeker) error {
	id, err := c.CreateMultipartUpload(bucket, key)
	if err != nil {
		return fmt.Errorf("failed to start multipart upload: %w", err)
	}

	parts, err := c.listParts(bucket, key, azblob.BlockListTypeCommitted)
	// If the given object does not exist, we create it later in the function instead of failing as defined by the
	// 'Client' interface
	if err != nil && !objerr.IsNotFoundError(err) {
		return fmt.Errorf("failed to get parts that are already committed to blob: %w", err)
	}

	newPart, err := c.UploadPart(bucket, id, key, objcli.NoPartNumber, data)
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	parts = append(parts, newPart)

	err = c.CompleteMultipartUpload(bucket, id, key, parts...)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (c *Client) DeleteObjects(bucket string, keys ...string) error {
	containerClient, err := c.storageAPI.ToContainerAPI(bucket)
	if err != nil {
		return err // Purposefully not wrapped
	}

	pool := hofp.NewPool(hofp.Options{
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

func (c *Client) DeleteDirectory(bucket, prefix string) error {
	fn := func(attrs *objval.ObjectAttrs) error {
		return c.DeleteObjects(bucket, attrs.Key)
	}

	return c.IterateObjects(bucket, prefix, "", nil, nil, fn)
}

func (c *Client) IterateObjects(bucket, prefix, delimiter string, include, exclude []*regexp.Regexp,
	fn objcli.IterateFunc,
) error {
	if include != nil && exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	containerClient, err := c.storageAPI.ToContainerAPI(bucket)
	if err != nil {
		return err // Purposefully not wrapped
	}

	if delimiter == "" {
		return c.iterateObjectsFlat(containerClient, bucket, prefix, include, exclude, fn)
	}

	return c.iterateObjectsHierarchy(containerClient, bucket, prefix, delimiter, include, exclude, fn)
}

func (c *Client) iterateObjectsFlat(containerClient containerAPI, bucket, prefix string, include,
	exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	options := azblob.ContainerListBlobsFlatOptions{Prefix: &prefix}

	return c.iterateObjectsWithPager(
		containerClient.GetListBlobsFlatPagerAPI(options),
		bucket,
		include,
		exclude,
		fn,
	)
}

func (c *Client) iterateObjectsHierarchy(containerClient containerAPI, bucket, prefix, delimiter string, include,
	exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	options := azblob.ContainerListBlobsHierarchyOptions{Prefix: &prefix}

	return c.iterateObjectsWithPager(
		containerClient.GetListBlobsHierarchyPagerAPI(delimiter, options),
		bucket,
		include,
		exclude,
		fn,
	)
}

func (c *Client) iterateObjectsWithPager(pager listBlobsPagerAPI, bucket string, include,
	exclude []*regexp.Regexp, fn objcli.IterateFunc,
) error {
	var (
		ctx      = context.Background()
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

func (c *Client) CreateMultipartUpload(bucket, key string) (string, error) {
	return objcli.NoUploadID, nil
}

func (c *Client) ListParts(bucket, id, key string) ([]objval.Part, error) {
	if id != objcli.NoUploadID {
		return nil, objcli.ErrExpectedNoUploadID
	}

	return c.listParts(bucket, key, azblob.BlockListTypeUncommitted)
}

func (c *Client) listParts(bucket, key string, blockType azblob.BlockListType) ([]objval.Part, error) {
	blobClient, err := c.storageAPI.ToBlobAPI(bucket, key)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	resp, err := blobClient.GetBlockList(
		context.Background(),
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

func (c *Client) UploadPart(bucket, id, key string, number int, body io.ReadSeeker) (objval.Part, error) {
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
		context.Background(),
		blockID,
		body,
		azblob.BlockBlobStageBlockOptions{TransactionalContentMD5: md5sum.Sum(nil)},
	)

	return objval.Part{ID: blockID, Number: number, Size: size}, handleError(bucket, key, err)
}

func (c *Client) UploadPartCopy(bucket, id, dst, src string, number int, br *objval.ByteRange) (objval.Part, error) {
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

	srcClient, err := c.storageAPI.ToBlobAPI(bucket, src)
	if err != nil {
		return objval.Part{}, err
	}

	dstClient, err := c.storageAPI.ToBlobAPI(bucket, dst)
	if err != nil {
		return objval.Part{}, err
	}

	_, err = dstClient.StageBlockFromURL(
		context.Background(),
		blockID,
		srcClient.URL(),
		0, // Should be set to 0 (?) https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url
		azblob.BlockBlobStageBlockFromURLOptions{Offset: &offset, Count: &length},
	)
	if err != nil {
		return objval.Part{}, handleError(bucket, dst, err)
	}

	return objval.Part{ID: blockID, Number: number, Size: length}, nil
}

func (c *Client) CompleteMultipartUpload(bucket, id, key string, parts ...objval.Part) error {
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
		context.Background(),
		converted,
		azblob.BlockBlobCommitBlockListOptions{},
	)

	return handleError(bucket, key, err)
}

func (c *Client) AbortMultipartUpload(_, id, _ string) error {
	if id != objcli.NoUploadID {
		return objcli.ErrExpectedNoUploadID
	}

	// NOTE: Azure doesn't support removing/cleaning up staged blocks; it automatically garbage collects them after a
	// certain amount of time.

	return nil
}
