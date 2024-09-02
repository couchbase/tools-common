// Package objazure provides an implementation of 'objstore.Client' for use with Azure blob storage.
package objazure

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/google/uuid"

	"github.com/couchbase/tools-common/cloud/v6/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v6/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v6/objstore/objval"
	"github.com/couchbase/tools-common/sync/v2/hofp"
	"github.com/couchbase/tools-common/types/ptr"
	"github.com/couchbase/tools-common/utils/v3/system"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// sasErrString is the error message returned when GetSASURL fails because the client was not authenticated with a
// shared key. See
// https://github.com/Azure/azure-sdk-for-go/blob/2f352faf4f49cd23c53ffcce0aea62da3cdfa03d/sdk/storage/azblob/blob/client.go#L246
// and MB-55302.
//
//nolint:lll
const sasErrString = "credential is not a SharedKeyCredential. SAS can only be signed with a SharedKeyCredential"

// NOTE: As apposed to AWS/GCP, Azure use the container/blob naming convention, however, for consistency the Azure
// client implementation continues to use the bucket/key names.

// attrs wraps object attributes with internal attributes (e.g. version).
type attrs struct {
	objval.ObjectAttrs
	Version *string
}

// Client implements the 'objcli.Client' interface allowing the creation/management of blobs stored in Azure blob store.
type Client struct {
	serviceAPI serviceAPI
}

var _ objcli.Client = (*Client)(nil)

// ClientOptions encapsulates the options for creating a new Azure Client.
type ClientOptions struct {
	// Client represents a URL to the Azure Blob Storage service allowing you to manipulate blob containers.
	//
	// NOTE: Required
	Client *service.Client
}

// NewClient returns a new client which uses the given service client, in general this should be the one created using
// the 'azblob.NewServiceClient' function exposed by the SDK.
func NewClient(options ClientOptions) *Client {
	return &Client{serviceAPI: &serviceClient{client: options.Client}}
}

func (c *Client) getBlobBlockClient(bucket, key string) blockBlobAPI {
	return c.serviceAPI.NewContainerClient(bucket).NewBlockBlobClient(key)
}

func (c *Client) getBlobBlockVersionClient(bucket, key, version string) (blockBlobAPI, error) {
	return c.serviceAPI.NewContainerClient(bucket).NewBlockBlobVersionClient(key, version)
}

func (c *Client) Provider() objval.Provider {
	return objval.ProviderAzure
}

func (c *Client) GetObject(ctx context.Context, opts objcli.GetObjectOptions) (*objval.Object, error) {
	if err := opts.ByteRange.Valid(false); err != nil {
		return nil, err // Purposefully not wrapped
	}

	var offset, length int64 = 0, blob.CountToEnd
	if opts.ByteRange != nil {
		offset, length = opts.ByteRange.ToOffsetLength(length)
	}

	blobClient := c.getBlobBlockClient(opts.Bucket, opts.Key)

	resp, err := blobClient.DownloadStream(
		ctx,
		&blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset: offset, Count: length}},
	)
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	attrs := objval.ObjectAttrs{
		Key:          opts.Key,
		Size:         resp.ContentLength,
		LastModified: resp.LastModified,
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        resp.Body,
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(ctx context.Context, opts objcli.GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	blobClient := c.getBlobBlockClient(opts.Bucket, opts.Key)

	resp, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          opts.Key,
		ETag:         (*string)(resp.ETag),
		Size:         resp.ContentLength,
		LastModified: resp.LastModified,
	}

	return attrs, nil
}

func (c *Client) PutObject(ctx context.Context, opts objcli.PutObjectOptions) error {
	blobClient := c.getBlobBlockClient(opts.Bucket, opts.Key)

	md5sum := md5.New()

	_, err := objcli.CopyReadSeeker(md5sum, opts.Body)
	if err != nil {
		return fmt.Errorf("failed to calculate checksums: %w", err)
	}

	_, err = blobClient.Upload(
		ctx,
		manager.ReadSeekCloser(opts.Body),
		&blockblob.UploadOptions{TransactionalValidation: blob.TransferValidationTypeMD5(md5sum.Sum(nil))},
	)

	return handleError(opts.Bucket, opts.Key, err)
}

func (c *Client) CopyObject(ctx context.Context, opts objcli.CopyObjectOptions) error {
	dstClient := c.serviceAPI.NewContainerClient(opts.DestinationBucket).NewBlobClient(opts.DestinationKey)

	srcURL, err := c.getSASURL(opts.SourceBucket, opts.SourceKey)
	if err != nil {
		return fmt.Errorf("failed to get the source object URL: %w", err)
	}

	_, err = dstClient.CopyFromURL(ctx, srcURL, &blob.CopyFromURLOptions{})

	return handleError("", "", err)
}

func (c *Client) AppendToObject(ctx context.Context, opts objcli.AppendToObjectOptions) error {
	attrs, err := c.GetObjectAttrs(ctx, objcli.GetObjectAttrsOptions{
		Bucket: opts.Bucket,
		Key:    opts.Key,
	})

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) || attrs != nil && ptr.From(attrs.Size) == 0 {
		return c.PutObject(ctx, objcli.PutObjectOptions(opts))
	}

	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
	}

	id, err := c.CreateMultipartUpload(ctx, objcli.CreateMultipartUploadOptions{
		Bucket: opts.Bucket,
		Key:    opts.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to start multipart upload: %w", err)
	}

	existing, err := c.UploadPartCopy(ctx, objcli.UploadPartCopyOptions{
		DestinationBucket: opts.Bucket,
		UploadID:          id,
		DestinationKey:    opts.Key,
		SourceBucket:      opts.Bucket,
		SourceKey:         opts.Key,
		Number:            objcli.NoPartNumber,
	})
	if err != nil {
		return fmt.Errorf("failed to get existing object part: %w", err)
	}

	intermediate, err := c.UploadPart(ctx, objcli.UploadPartOptions{
		Bucket:   opts.Bucket,
		UploadID: id,
		Key:      opts.Key,
		Number:   objcli.NoPartNumber,
		Body:     opts.Body,
	})
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	err = c.CompleteMultipartUpload(ctx, objcli.CompleteMultipartUploadOptions{
		Bucket:   opts.Bucket,
		UploadID: id,
		Key:      opts.Key,
		Parts:    []objval.Part{existing, intermediate},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (c *Client) DeleteObjects(ctx context.Context, opts objcli.DeleteObjectsOptions) error {
	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(opts.Keys)),
	})

	del := func(ctx context.Context, key string) error {
		blobClient := c.getBlobBlockClient(opts.Bucket, key)

		_, err := blobClient.Delete(ctx, &blob.DeleteOptions{})
		if err != nil && !isKeyNotFound(err) {
			return handleError(opts.Bucket, key, err)
		}

		return nil
	}

	queue := func(key string) error {
		return pool.Queue(func(ctx context.Context) error { return del(ctx, key) })
	}

	for _, key := range opts.Keys {
		if queue(key) != nil {
			break
		}
	}

	return pool.Stop()
}

func (c *Client) DeleteDirectory(ctx context.Context, opts objcli.DeleteDirectoryOptions) error {
	var (
		// size matches the batch deletion size in AWS/Azure.
		size  = 1000
		batch = make([]attrs, 0, size)
	)

	fn := func(obj attrs) error {
		batch = append(batch, obj)

		if len(batch) < size {
			return nil
		}

		err := c.deleteObjects(ctx, opts.Bucket, batch...)
		if err != nil {
			return fmt.Errorf("failed to delete batch: %w", err)
		}

		clear(batch)
		batch = batch[:0]

		return nil
	}

	err := c.iterateObjects(
		ctx,
		opts.Bucket,
		opts.Prefix,
		"",
		opts.Versions,
		nil,
		nil,
		fn,
	)
	if err != nil {
		return fmt.Errorf("failed to iterate objects: %w", err)
	}

	// Ensure we flush the last batch
	err = c.deleteObjects(ctx, opts.Bucket, batch...)
	if err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return nil
}

func (c *Client) deleteObjects(ctx context.Context, bucket string, objects ...attrs) error {
	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(objects)),
	})

	del := func(ctx context.Context, obj attrs) error {
		blobClient, err := c.getBlobBlockVersionClient(bucket, obj.Key, ptr.From(obj.Version))
		if err != nil {
			return handleError(bucket, obj.Key, err)
		}

		_, err = blobClient.Delete(ctx, &blob.DeleteOptions{})
		if err != nil && !isKeyNotFound(err) {
			return handleError(bucket, obj.Key, err)
		}

		return nil
	}

	queue := func(obj attrs) error {
		return pool.Queue(func(ctx context.Context) error { return del(ctx, obj) })
	}

	for _, obj := range objects {
		if queue(obj) != nil {
			break
		}
	}

	return pool.Stop()
}

// Close is a no-op for Azure as this won't result in a memory leak.
func (c *Client) Close() error {
	return nil
}

func (c *Client) IterateObjects(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	var (
		bucket    = opts.Bucket
		prefix    = opts.Prefix
		delimiter = opts.Delimiter
		include   = opts.Include
		exclude   = opts.Exclude
		fn        = func(obj attrs) error { return opts.Func(&obj.ObjectAttrs) }
	)

	return c.iterateObjects(ctx, bucket, prefix, delimiter, false, include, exclude, fn)
}

// iterateObjects is an internal object iteration function which also support enabling listing object versions.
func (c *Client) iterateObjects(
	ctx context.Context,
	bucket string,
	prefix string,
	delimiter string,
	versions bool,
	include []*regexp.Regexp,
	exclude []*regexp.Regexp,
	fn func(attrs) error,
) error {
	if include != nil && exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	containerClient := c.serviceAPI.NewContainerClient(bucket)

	if delimiter == "" {
		return c.iterateObjectsFlat(ctx, containerClient, bucket, prefix, versions, include, exclude, fn)
	}

	return c.iterateObjectsHierarchy(ctx, containerClient, bucket, prefix, delimiter, versions, include, exclude, fn)
}

// iterateObjectsFlat iterates the given prefix as if it were flat (e.g. recursively).
func (c *Client) iterateObjectsFlat(
	ctx context.Context,
	containerClient containerAPI,
	bucket, prefix string,
	versions bool,
	include, exclude []*regexp.Regexp,
	fn func(attrs) error,
) error {
	options := container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Versions: versions},
	}

	pager := containerClient.NewListBlobsFlatPager(&options)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return handleError(bucket, "", err)
		}

		var prefixes []*string

		objects := c.blobsToAttrs(prefixes, resp.Segment.BlobItems)

		err = c.iterateSegment(objects, include, exclude, fn)
		if err != nil {
			return handleError(bucket, "", err)
		}
	}

	return nil
}

// iterateObjectsHierarchy iterates the given "directory".
func (c *Client) iterateObjectsHierarchy(
	ctx context.Context,
	containerClient containerAPI,
	bucket, prefix, delimiter string,
	versions bool,
	include, exclude []*regexp.Regexp,
	fn func(attrs) error,
) error {
	options := container.ListBlobsHierarchyOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Versions: versions},
	}

	pager := containerClient.NewListBlobsHierarchyPager(delimiter, &options)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return handleError(bucket, "", err)
		}

		prefixes, blobs := resp.Segment.BlobPrefixes, resp.Segment.BlobItems

		// The type of BlobPrefixes (BlobPrefix) is not exported from the Azure SDK. That means we can't use it as a
		// parameter type in convertBlobsToObjectAttrs. To work around this, turn the prefixes to strings.
		stringPrefixes := make([]*string, len(prefixes))

		for i, prefix := range prefixes {
			stringPrefixes[i] = prefix.Name
		}

		objects := c.blobsToAttrs(stringPrefixes, blobs)

		err = c.iterateSegment(objects, include, exclude, fn)
		if err != nil {
			return handleError(bucket, "", err)
		}
	}

	return nil
}

// blobsToAttrs converts the given blob items into internal objects.
func (c *Client) blobsToAttrs(prefixes []*string, blobs []*container.BlobItem) []attrs {
	converted := make([]attrs, 0, len(prefixes)+len(blobs))

	for _, p := range prefixes {
		converted = append(converted, attrs{ObjectAttrs: objval.ObjectAttrs{Key: *p}})
	}

	for _, b := range blobs {
		oa := objval.ObjectAttrs{
			Key:          *b.Name,
			Size:         b.Properties.ContentLength,
			LastModified: b.Properties.LastModified,
		}

		attrs := attrs{
			ObjectAttrs: oa,
			Version:     b.VersionID,
		}

		converted = append(converted, attrs)
	}

	return converted
}

// iterateSegment iterates over the given segment (<=5000) of objects executing the given function for each object which
// has not been explicitly ignored by the user.
func (c *Client) iterateSegment(
	objects []attrs,
	include, exclude []*regexp.Regexp,
	fn func(attrs) error,
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

func (c *Client) CreateMultipartUpload(_ context.Context, _ objcli.CreateMultipartUploadOptions) (string, error) {
	return objcli.NoUploadID, nil
}

func (c *Client) ListParts(ctx context.Context, opts objcli.ListPartsOptions) ([]objval.Part, error) {
	if opts.UploadID != objcli.NoUploadID {
		return nil, objcli.ErrExpectedNoUploadID
	}

	return c.listParts(ctx, opts.Bucket, opts.Key, blockblob.BlockListTypeUncommitted)
}

func (c *Client) listParts(
	ctx context.Context,
	bucket, key string,
	blockType blockblob.BlockListType,
) ([]objval.Part, error) {
	blobClient := c.getBlobBlockClient(bucket, key)

	resp, err := blobClient.GetBlockList(
		ctx,
		blockType,
		&blockblob.GetBlockListOptions{},
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

func (c *Client) UploadPart(ctx context.Context, opts objcli.UploadPartOptions) (objval.Part, error) {
	if opts.UploadID != objcli.NoUploadID {
		return objval.Part{}, objcli.ErrExpectedNoUploadID
	}

	size, err := objcli.SeekerLength(opts.Body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to determine body length: %w", err)
	}

	var (
		md5sum  = md5.New()
		blockID = base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))
	)

	blobClient := c.getBlobBlockClient(opts.Bucket, opts.Key)

	_, err = objcli.CopyReadSeeker(md5sum, opts.Body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to calculate checksums: %w", err)
	}

	_, err = blobClient.StageBlock(
		ctx,
		blockID,
		manager.ReadSeekCloser(opts.Body),
		&blockblob.StageBlockOptions{TransactionalValidation: blob.TransferValidationTypeMD5(md5sum.Sum(nil))},
	)

	part := objval.Part{
		ID:     blockID,
		Number: opts.Number,
		Size:   size,
	}

	return part, handleError(opts.Bucket, opts.Key, err)
}

func (c *Client) UploadPartCopy(ctx context.Context, opts objcli.UploadPartCopyOptions) (objval.Part, error) {
	if opts.UploadID != objcli.NoUploadID {
		return objval.Part{}, objcli.ErrExpectedNoUploadID
	}

	if err := opts.ByteRange.Valid(false); err != nil {
		return objval.Part{}, err // Purposefully not wrapped
	}

	var offset, length int64 = 0, blob.CountToEnd
	if opts.ByteRange != nil {
		offset, length = opts.ByteRange.ToOffsetLength(length)
	}

	blockID := base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))

	srcURL, err := c.getSASURL(opts.SourceBucket, opts.SourceKey)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to get the source part URL: %w", err)
	}

	dstClient := c.getBlobBlockClient(opts.DestinationBucket, opts.DestinationKey)

	_, err = dstClient.StageBlockFromURL(
		ctx,
		blockID,
		srcURL,
		&blockblob.StageBlockFromURLOptions{Range: blob.HTTPRange{Offset: offset, Count: length}},
	)
	if err != nil {
		return objval.Part{}, handleError(opts.DestinationBucket, opts.DestinationKey, err)
	}

	return objval.Part{ID: blockID, Number: opts.Number, Size: length}, nil
}

func (c *Client) getSASURL(bucket, src string) (string, error) {
	var (
		srcContainerClient = c.serviceAPI.NewContainerClient(bucket)
		srcClient          = srcContainerClient.NewBlobClient(src)
		permissions        = sas.BlobPermissions{Read: true}
		start              = time.Now().UTC()
		expiry             = start.Add(48 * time.Hour)
	)

	opts := blob.GetSASURLOptions{StartTime: &start}

	url, err := srcClient.GetSASURL(permissions, expiry, &opts)

	if err == nil {
		return url, nil
	}

	// We only need a SAS token when the service client is using a shared key. Unfortunately this version of the SDK does
	// not provide a method of finding this out directly. The call to GetSASURL will check it is the case however, but it
	// does not export the error returned. We therefore must resort to a string comparison on the error. See MB-55302.
	if err.Error() == sasErrString {
		return c.getBlobBlockClient(bucket, src).URL(), nil
	}

	return "", fmt.Errorf("failed to get SAS URL: %w", err)
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, opts objcli.CompleteMultipartUploadOptions) error {
	if opts.UploadID != objcli.NoUploadID {
		return objcli.ErrExpectedNoUploadID
	}

	blobClient := c.getBlobBlockClient(opts.Bucket, opts.Key)

	converted := make([]string, 0, len(opts.Parts))

	for _, part := range opts.Parts {
		converted = append(converted, part.ID)
	}

	_, err := blobClient.CommitBlockList(
		ctx,
		converted,
		&blockblob.CommitBlockListOptions{},
	)

	return handleError(opts.Bucket, opts.Key, err)
}

func (c *Client) AbortMultipartUpload(_ context.Context, opts objcli.AbortMultipartUploadOptions) error {
	if opts.UploadID != objcli.NoUploadID {
		return objcli.ErrExpectedNoUploadID
	}

	// NOTE: Azure doesn't support removing/cleaning up staged blocks; it automatically garbage collects them after a
	// certain amount of time.

	return nil
}
