// Package objazure provides an implementation of 'objstore.Client' for use with Azure blob storage.
package objazure

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/google/uuid"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	"github.com/couchbase/tools-common/sync/v2/hofp"
	"github.com/couchbase/tools-common/types/v2/ptr"
	"github.com/couchbase/tools-common/types/v2/timeprovider"
	"github.com/couchbase/tools-common/utils/v3/system"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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

// Client implements the 'objcli.Client' interface allowing the creation/management of blobs stored in Azure blob store.
type Client struct {
	serviceAPI serviceAPI

	// timeProvider is an abstraction which provides the current time. We need this for testing.
	timeProvider timeprovider.TimeProvider
}

var _ objcli.Client = (*Client)(nil)

// ClientOptions encapsulates the options for creating a new Azure Client.
type ClientOptions struct {
	// Client represents a URL to the Azure Blob Storage service allowing you to manipulate blob containers.
	//
	// NOTE: Required
	Client *service.Client

	// timeProvider is an abstraction which provides the current time. We need this for testing.
	timeProvider timeprovider.TimeProvider
}

// NewClient returns a new client which uses the given service client, in general this should be the one created using
// the 'azblob.NewServiceClient' function exposed by the SDK.
func NewClient(options ClientOptions) *Client {
	client := Client{serviceAPI: &serviceClient{client: options.Client}, timeProvider: options.timeProvider}

	if client.timeProvider == nil {
		client.timeProvider = timeprovider.CurrentTimeProvider{}
	}

	return &client
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

	var blobClient blockBlobAPI

	if opts.VersionID == "" {
		blobClient = c.getBlobBlockClient(opts.Bucket, opts.Key)
	} else {
		var err error

		blobClient, err = c.getBlobBlockVersionClient(opts.Bucket, opts.Key, opts.VersionID)
		if err != nil {
			return nil, handleError(opts.Bucket, opts.Key, fmt.Errorf("failed to get blob version client: %w", err))
		}
	}

	resp, err := blobClient.DownloadStream(
		ctx,
		&blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset: offset, Count: length}},
	)
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	attrs := objval.ObjectAttrs{
		Key:            opts.Key,
		Size:           resp.ContentLength,
		LastModified:   resp.LastModified,
		LockExpiration: resp.ImmutabilityPolicyExpiresOn,
	}

	if resp.ImmutabilityPolicyMode != nil {
		attrs.LockType = getLockType(*resp.ImmutabilityPolicyMode)
	}

	if resp.VersionID != nil {
		attrs.VersionID = *resp.VersionID
	}

	if resp.ETag != nil {
		attrs.CAS = string(*resp.ETag)
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        resp.Body,
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(ctx context.Context, opts objcli.GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	var blobClient blockBlobAPI

	if opts.VersionID == "" {
		blobClient = c.getBlobBlockClient(opts.Bucket, opts.Key)
	} else {
		var err error

		blobClient, err = c.getBlobBlockVersionClient(opts.Bucket, opts.Key, opts.VersionID)
		if err != nil {
			return nil, handleError(opts.Bucket, opts.Key, fmt.Errorf("failed to get blob version client: %w", err))
		}
	}

	resp, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:            opts.Key,
		ETag:           (*string)(resp.ETag),
		Size:           resp.ContentLength,
		LastModified:   resp.LastModified,
		LockExpiration: resp.ImmutabilityPolicyExpiresOn,
	}

	if resp.ImmutabilityPolicyMode != nil {
		attrs.LockType = getLockType(*resp.ImmutabilityPolicyMode)
	}

	if resp.VersionID != nil {
		attrs.VersionID = *resp.VersionID
	}

	if resp.ETag != nil {
		attrs.CAS = string(*resp.ETag)
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

	inputOpts := &blockblob.UploadOptions{
		TransactionalValidation: blob.TransferValidationTypeMD5(md5sum.Sum(nil)),
	}

	switch opts.Precondition {
	case objcli.OperationPreconditionOnlyIfAbsent:
		inputOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: ptr.To(azcore.ETagAny),
			},
		}
	case objcli.OperationPreconditionIfMatch:
		inputOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: ptr.To(azcore.ETag(opts.PreconditionData)),
			},
		}
	}

	if opts.Lock != nil {
		switch opts.Lock.Type {
		case objval.LockTypeCompliance:
			inputOpts.ImmutabilityPolicyMode = ptr.To(blob.ImmutabilityPolicySettingLocked)
			inputOpts.ImmutabilityPolicyExpiryTime = ptr.To(opts.Lock.Expiration)
		default:
			return errors.New("unsupported lock type")
		}
	}

	_, err = blobClient.Upload(
		ctx,
		manager.ReadSeekCloser(opts.Body),
		inputOpts,
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
		putOpts := objcli.PutObjectOptions{
			Bucket: opts.Bucket,
			Key:    opts.Key,
			Body:   opts.Body,
		}

		err := c.PutObject(ctx, putOpts)
		if err != nil {
			return fmt.Errorf("failed to put object: %w", err)
		}

		return nil
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

func (c *Client) DeleteObjectVersions(ctx context.Context, opts objcli.DeleteObjectVersionsOptions) error {
	objVersions := make([]objval.ObjectAttrs, 0)

	for _, objVersion := range opts.Versions {
		objVersions = append(objVersions, objval.ObjectAttrs{
			Key:       objVersion.Key,
			VersionID: objVersion.VersionID,
		})
	}

	return c.deleteObjects(ctx, opts.Bucket, objVersions...)
}

func (c *Client) DeleteDirectory(ctx context.Context, opts objcli.DeleteDirectoryOptions) error {
	var (
		// size matches the batch deletion size in AWS/Azure.
		size            = 1000
		nonCurrentBatch = make([]objval.ObjectAttrs, 0, size)
		currentBatch    = make([]objval.ObjectAttrs, 0, size)
	)

	deleteBatch := func() error {
		if len(currentBatch) > 0 {
			err := c.deleteObjects(ctx, opts.Bucket, currentBatch...)
			if err != nil {
				return err
			}
		}

		if len(nonCurrentBatch) > 0 {
			err := c.deleteObjects(ctx, opts.Bucket, nonCurrentBatch...)
			if err != nil {
				return err
			}
		}

		return nil
	}

	fn := func(obj objval.ObjectAttrs) error {
		if obj.LockExpiration != nil && obj.LockExpiration.After(c.timeProvider.Now()) {
			return objerr.ErrDeleteDirectoryRemainingItems{Bucket: opts.Bucket, Key: obj.Key}
		}

		// Before we can perform a version delete on the current version of an object we must mark the object as
		// deleted. If 'Versions' is set to 'true' we need to delete the 'currentBatch' before the 'nonCurrentBatch'.
		if obj.IsCurrentVersion {
			currentBatch = append(currentBatch, objval.ObjectAttrs{Key: obj.Key})
		}

		if opts.Versions {
			nonCurrentBatch = append(nonCurrentBatch, obj)
		}

		if len(nonCurrentBatch) < size || len(currentBatch) < size {
			return nil
		}

		err := deleteBatch()
		if err != nil {
			return fmt.Errorf("failed to delete batch: %w", err)
		}

		clear(currentBatch)
		clear(nonCurrentBatch)
		nonCurrentBatch = nonCurrentBatch[:0]
		currentBatch = currentBatch[:0]

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
	err = deleteBatch()
	if err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return nil
}

func (c *Client) deleteObjects(ctx context.Context, bucket string, objects ...objval.ObjectAttrs) error {
	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(objects)),
	})

	del := func(ctx context.Context, obj objval.ObjectAttrs) error {
		var (
			blobClient blockBlobAPI
			err        error
		)

		if obj.VersionID == "" {
			blobClient = c.getBlobBlockClient(bucket, obj.Key)
		} else {
			blobClient, err = c.getBlobBlockVersionClient(bucket, obj.Key, obj.VersionID)
			if err != nil {
				return handleError(bucket, obj.Key, fmt.Errorf("failed to get blob version client: %w", err))
			}
		}

		_, err = blobClient.Delete(ctx, &blob.DeleteOptions{})
		if err != nil && !isKeyNotFound(err) {
			return handleError(bucket, obj.Key, fmt.Errorf("failed to delete blob: %w", err))
		}

		return nil
	}

	queue := func(obj objval.ObjectAttrs) error {
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
		fn        = func(obj objval.ObjectAttrs) error { return opts.Func(&obj) }
	)

	return c.iterateObjects(ctx, bucket, prefix, delimiter, opts.Versions, include, exclude, fn)
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
	fn func(objval.ObjectAttrs) error,
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
	fn func(objval.ObjectAttrs) error,
) error {
	options := container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Versions: versions, ImmutabilityPolicy: true},
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
	fn func(objval.ObjectAttrs) error,
) error {
	options := container.ListBlobsHierarchyOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Versions: versions, ImmutabilityPolicy: true},
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
func (c *Client) blobsToAttrs(prefixes []*string, blobs []*container.BlobItem) []objval.ObjectAttrs {
	converted := make([]objval.ObjectAttrs, 0, len(prefixes)+len(blobs))

	for _, p := range prefixes {
		converted = append(converted, objval.ObjectAttrs{Key: *p})
	}

	for _, b := range blobs {
		attrs := objval.ObjectAttrs{
			Key:          *b.Name,
			Size:         b.Properties.ContentLength,
			LastModified: b.Properties.LastModified,
		}

		if b.VersionID != nil {
			attrs.VersionID = *b.VersionID
		}

		if b.IsCurrentVersion != nil {
			attrs.IsCurrentVersion = *b.IsCurrentVersion
		}

		if b.Properties != nil {
			attrs.LockExpiration = b.Properties.ImmutabilityPolicyExpiresOn

			if b.Properties.ImmutabilityPolicyMode != nil {
				attrs.LockType = getLockType(*b.Properties.ImmutabilityPolicyMode)
			}
		}

		converted = append(converted, attrs)
	}

	return converted
}

// iterateSegment iterates over the given segment (<=5000) of objects executing the given function for each object which
// has not been explicitly ignored by the user.
func (c *Client) iterateSegment(
	objects []objval.ObjectAttrs,
	include, exclude []*regexp.Regexp,
	fn func(objval.ObjectAttrs) error,
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
		start              = c.timeProvider.Now().UTC()
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

	inputOpts := &blockblob.CommitBlockListOptions{}

	if opts.Precondition == objcli.OperationPreconditionOnlyIfAbsent {
		inputOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: ptr.To(azcore.ETagAny),
			},
		}
	}

	if opts.Lock != nil {
		switch opts.Lock.Type {
		case objval.LockTypeCompliance:
			inputOpts.ImmutabilityPolicyMode = ptr.To(blob.ImmutabilityPolicySettingLocked)
			inputOpts.ImmutabilityPolicyExpiryTime = ptr.To(opts.Lock.Expiration)
		default:
			return errors.New("unsupported lock type")
		}
	}

	_, err := blobClient.CommitBlockList(
		ctx,
		converted,
		inputOpts,
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

func (c *Client) GetBucketLockingStatus(
	ctx context.Context,
	opts objcli.GetBucketLockingStatusOptions,
) (*objval.BucketLockingStatus, error) {
	containerClient := c.serviceAPI.NewContainerClient(opts.Bucket)

	output, err := containerClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, handleError(opts.Bucket, "", err)
	}

	result := &objval.BucketLockingStatus{}
	if output.IsImmutableStorageWithVersioningEnabled != nil {
		result.Enabled = *output.IsImmutableStorageWithVersioningEnabled
	}

	return result, nil
}

func (c *Client) SetObjectLock(ctx context.Context, opts objcli.SetObjectLockOptions) error {
	if opts.Lock == nil {
		return errors.New("object lock is nil")
	}

	blobClient := c.serviceAPI.NewContainerClient(opts.Bucket).NewBlobClient(opts.Key)

	var err error
	if opts.VersionID != "" {
		blobClient, err = blobClient.WithVersionID(opts.VersionID)
		if err != nil {
			return handleError(opts.Bucket, opts.Key, fmt.Errorf("failed to retrieve version object handle: %w", err))
		}
	}

	switch opts.Lock.Type {
	case objval.LockTypeCompliance:
		inputOpts := &blob.SetImmutabilityPolicyOptions{
			Mode: ptr.To(blob.ImmutabilityPolicySettingLocked),
		}

		_, err = blobClient.SetImmutabilityPolicy(
			ctx,
			opts.Lock.Expiration,
			inputOpts,
		)
		if err != nil {
			return handleError(opts.Bucket, opts.Key, fmt.Errorf("failed to set immutability policy: %w", err))
		}
	default:
		return errors.New("unsupported lock type")
	}

	return nil
}

// getLockType converts Azure's 'blob.ImmutabilityPolicyMode' to 'objval.LockType'.
//
// NOTE: The value of the blob.ImmutabilityPolicyModeLocked constant is capitalized ("Locked"), however the Azure API
// always returns a lowercase string ("locked") so the values will never match unless we manually convert them.
func getLockType(azureLockMode blob.ImmutabilityPolicyMode) objval.LockType {
	switch strings.ToLower(string(azureLockMode)) {
	case strings.ToLower(string(blob.ImmutabilityPolicyModeLocked)):
		return objval.LockTypeCompliance
	default:
		return objval.LockTypeUndefined
	}
}
