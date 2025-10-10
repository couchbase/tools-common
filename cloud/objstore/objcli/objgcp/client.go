// Package objgcp provides an implementation of 'objstore.Client' for use with GCS.
package objgcp

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"regexp"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
	"google.golang.org/api/iterator"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	"github.com/couchbase/tools-common/sync/v2/hofp"
	"github.com/couchbase/tools-common/types/v2/ptr"
	"github.com/couchbase/tools-common/types/v2/timeprovider"
	"github.com/couchbase/tools-common/utils/v3/system"
)

type composeOptions struct {
	bucket       string
	key          string
	parts        []string
	precondition objcli.OperationPrecondition
	lock         *objcli.ObjectLock
}

// Client implements the 'objcli.Client' interface allowing the creation/management of objects stored in Google Storage.
type Client struct {
	serviceAPI serviceAPI
	logger     *slog.Logger

	// timeProvider is an abstraction which provides the current time. We need this for testing.
	timeProvider timeprovider.TimeProvider
}

var _ objcli.Client = (*Client)(nil)

// ClientOptions encapsulates the options for creating a new GCP Client.
type ClientOptions struct {
	// Client is a client for interacting with Google Cloud Storage.
	//
	// NOTE: Required
	Client *storage.Client

	// Logger is the passed logger which implements a custom Log method
	Logger *slog.Logger

	// timeProvider is an abstraction which provides the current time. We need this for testing.
	timeProvider timeprovider.TimeProvider
}

// defaults fills any missing attributes to a sane default.
func (c *ClientOptions) defaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// NewClient returns a new client which uses the given storage client, in general this should be the one created using
// the 'storage.NewClient' function exposed by the SDK.
func NewClient(options ClientOptions) *Client {
	// Fill out any missing fields with the sane defaults
	options.defaults()

	client := Client{
		serviceAPI:   serviceClient{options.Client},
		logger:       options.Logger,
		timeProvider: options.timeProvider,
	}

	if client.timeProvider == nil {
		client.timeProvider = timeprovider.CurrentTimeProvider{}
	}

	return &client
}

func (c *Client) Provider() objval.Provider {
	return objval.ProviderGCP
}

func (c *Client) GetObject(ctx context.Context, opts objcli.GetObjectOptions) (*objval.Object, error) {
	if err := opts.ByteRange.Valid(false); err != nil {
		return nil, err // Purposefully not wrapped
	}

	var offset, length int64 = 0, -1
	if opts.ByteRange != nil {
		offset, length = opts.ByteRange.ToOffsetLength(length)
	}

	object := c.serviceAPI.Bucket(opts.Bucket).Object(opts.Key)

	if opts.VersionID != "" {
		gen, err := strconv.ParseUint(opts.VersionID, 10, 64)
		if err != nil {
			return nil, handleError(
				opts.Bucket,
				opts.Key,
				fmt.Errorf("failed to parse VersionID into GCP generation: %w", err),
			)
		}

		object = object.Generation(int64(gen))
	}

	reader, err := object.NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	remote := reader.Attrs()

	attrs := objval.ObjectAttrs{
		Key:          opts.Key,
		Size:         ptr.To(remote.Size),
		LastModified: ptr.To(remote.LastModified),
	}

	if remote.Generation != 0 {
		v := strconv.FormatInt(remote.Generation, 10)
		attrs.VersionID = v
		attrs.CAS = v
	}

	objectRes := &objval.Object{
		ObjectAttrs: attrs,
		Body:        reader,
	}

	return objectRes, nil
}

func (c *Client) GetObjectAttrs(ctx context.Context, opts objcli.GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	object := c.serviceAPI.Bucket(opts.Bucket).Object(opts.Key)

	if opts.VersionID != "" {
		gen, err := strconv.ParseUint(opts.VersionID, 10, 64)
		if err != nil {
			return nil, handleError(
				opts.Bucket,
				opts.Key,
				fmt.Errorf("failed to parse VersionID into GCP generation: %w", err),
			)
		}

		object = object.Generation(int64(gen))
	}

	remote, err := object.Attrs(ctx)
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          opts.Key,
		ETag:         ptr.To(remote.Etag),
		Size:         ptr.To(remote.Size),
		LastModified: &remote.Updated,
	}

	if remote.Retention != nil {
		attrs.LockExpiration = &remote.Retention.RetainUntil
		attrs.LockType = getLockType(remote.Retention.Mode)
	}

	if remote.Generation != 0 {
		v := strconv.FormatInt(remote.Generation, 10)
		attrs.VersionID = v
		attrs.CAS = v
	}

	return attrs, nil
}

func (c *Client) PutObject(ctx context.Context, opts objcli.PutObjectOptions) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	var (
		md5sum = md5.New()
		crc32c = crc32.New(crc32.MakeTable(crc32.Castagnoli))
		// We always want to retry failed 'PutObject' requests, we generally have a lockfile which ensures (or we make
		// the assumption) that we have exclusive access to a given path prefix in GCP so we don't need to worry about
		// potentially overwriting objects.
		object = c.serviceAPI.Bucket(opts.Bucket).Object(opts.Key).Retryer(storage.WithPolicy(storage.RetryAlways))
	)

	switch opts.Precondition {
	case objcli.OperationPreconditionOnlyIfAbsent:
		object = object.If(storage.Conditions{DoesNotExist: true})
	case objcli.OperationPreconditionIfMatch:
		gen, err := strconv.ParseInt(opts.PreconditionData, 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse If-Match value as int64: %w", err)
		}

		object = object.If(storage.Conditions{GenerationMatch: gen})
	}

	writer := object.NewWriter(ctx)

	if opts.Lock != nil {
		err := writer.SetLock(opts.Lock)
		if err != nil {
			return fmt.Errorf("failed to set object lock: %w", err)
		}
	}

	_, err := objcli.CopyReadSeeker(io.MultiWriter(md5sum, crc32c), opts.Body)
	if err != nil {
		return fmt.Errorf("failed to calculate checksums: %w", err)
	}

	writer.SendMD5(md5sum.Sum(nil))
	writer.SendCRC(crc32c.Sum32())

	_, err = io.Copy(writer, opts.Body)
	if err != nil {
		return handleError(opts.Bucket, opts.Key, err)
	}

	return handleError(opts.Bucket, opts.Key, writer.Close())
}

func (c *Client) CopyObject(ctx context.Context, opts objcli.CopyObjectOptions) error {
	var (
		srcHandle = c.serviceAPI.Bucket(opts.SourceBucket).Object(opts.SourceKey)
		dstHandle = c.serviceAPI.Bucket(opts.DestinationBucket).Object(opts.DestinationKey)
	)

	// Copying is non-destructive from the source perspective and we don't mind potentially "overwriting" the
	// destination object, always retry.
	_, err := dstHandle.Retryer(storage.WithPolicy(storage.RetryAlways)).CopierFrom(srcHandle).Run(ctx)

	return handleError("", "", err)
}

func (c *Client) AppendToObject(ctx context.Context, opts objcli.AppendToObjectOptions) error {
	attrs, err := c.GetObjectAttrs(ctx, objcli.GetObjectAttrsOptions{
		Bucket: opts.Bucket,
		Key:    opts.Key,
	})

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) || ptr.From(attrs.Size) == 0 {
		putOpts := objcli.PutObjectOptions{
			Bucket: opts.Bucket,
			Key:    opts.Key,
			Body:   opts.Body,
		}

		return c.PutObject(ctx, putOpts)
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

	intermediate, err := c.UploadPart(ctx, objcli.UploadPartOptions{
		Bucket:   opts.Bucket,
		UploadID: id,
		Key:      opts.Key,
		Number:   2,
		Body:     opts.Body,
	})
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	part := objval.Part{
		ID:     opts.Key,
		Number: 1,
		Size:   ptr.From(attrs.Size),
	}

	err = c.CompleteMultipartUpload(ctx, objcli.CompleteMultipartUploadOptions{
		Bucket:   opts.Bucket,
		UploadID: id,
		Key:      opts.Key,
		Parts:    []objval.Part{part, intermediate},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (c *Client) DeleteObjects(ctx context.Context, opts objcli.DeleteObjectsOptions) error {
	objects := make([]objval.ObjectAttrs, 0, len(opts.Keys))

	for _, key := range opts.Keys {
		objects = append(objects, objval.ObjectAttrs{Key: key})
	}

	return c.deleteObjects(ctx, opts.Bucket, objects...)
}

func (c *Client) DeleteObjectVersions(ctx context.Context, opts objcli.DeleteObjectVersionsOptions) error {
	versions := make([]objval.ObjectAttrs, 0)

	for _, version := range opts.Versions {
		versions = append(versions, objval.ObjectAttrs{
			Key:       version.Key,
			VersionID: version.VersionID,
		})
	}

	return c.deleteObjects(ctx, opts.Bucket, versions...)
}

// deleteObjects uses a worker pool to delete the given objects.
func (c *Client) deleteObjects(ctx context.Context, bucket string, objects ...objval.ObjectAttrs) error {
	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(objects)),
	})

	del := func(ctx context.Context, object objval.ObjectAttrs) error {
		// We correctly handle the case where the object doesn't exist and should have exclusive access to the path
		// prefix in GCP, always retry.
		handle := c.serviceAPI.Bucket(bucket).Object(object.Key).Retryer(storage.WithPolicy(storage.RetryAlways))

		if object.VersionID != "" {
			gen, err := strconv.ParseUint(object.VersionID, 10, 64)
			if err != nil {
				return handleError(
					bucket,
					object.Key,
					fmt.Errorf("failed to parse VersionID into GCP generation: %w", err),
				)
			}

			handle = handle.Generation(int64(gen))
		}

		err := handle.Delete(ctx)

		if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return handleError(bucket, object.Key, err)
		}

		return nil
	}

	queue := func(object objval.ObjectAttrs) error {
		return pool.Queue(func(ctx context.Context) error { return del(ctx, object) })
	}

	for _, key := range objects {
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
		batch = make([]objval.ObjectAttrs, 0, size)
	)

	fn := func(obj objval.ObjectAttrs) error {
		if obj.LockExpiration != nil && obj.LockExpiration.After(c.timeProvider.Now()) {
			return objerr.ErrDeleteDirectoryRemainingItems{Bucket: opts.Bucket, Key: obj.Key}
		}

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

func (c *Client) IterateObjects(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	fn := func(attrs objval.ObjectAttrs) error {
		return opts.Func(&attrs)
	}

	return c.iterateObjects(ctx, opts.Bucket, opts.Prefix, opts.Delimiter, opts.Versions, opts.Include, opts.Exclude, fn)
}

// iterateObjects iterates through the objects in the remote storage allowing enabling listing object versions.
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

	query := &storage.Query{
		Prefix:     prefix,
		Delimiter:  delimiter,
		Projection: storage.ProjectionNoACL,
		Versions:   versions,
	}

	attrsToList := []string{
		"Name",
		"Etag",
		"Size",
		"Updated",
		"Retention",
	}

	if versions {
		attrsToList = append(attrsToList, "Generation")
	}

	err := query.SetAttrSelection(attrsToList)
	if err != nil {
		return fmt.Errorf("failed to set attribute selection: %w", err)
	}

	it := c.serviceAPI.Bucket(bucket).Objects(ctx, query)

	for {
		remote, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return handleError(bucket, "", fmt.Errorf("failed to get next object: %w", err))
		}

		if objcli.ShouldIgnore(remote.Name, include, exclude) {
			continue
		}

		var attrs *objval.ObjectAttrs

		// If Prefix is empty this isn't a directory stub, treat it as a normal object
		if remote.Prefix == "" {
			attrs = &objval.ObjectAttrs{
				Key:          remote.Name,
				Size:         ptr.To(remote.Size),
				LastModified: &remote.Updated,
			}

			if remote.Retention != nil {
				attrs.LockExpiration = &remote.Retention.RetainUntil
				attrs.LockType = getLockType(remote.Retention.Mode)
			}

			// If versions are enabled, populate the attribute
			if versions && remote.Generation != 0 {
				attrs.VersionID = strconv.FormatInt(remote.Generation, 10)
			}
		} else {
			attrs = &objval.ObjectAttrs{
				Key: remote.Prefix,
			}
		}

		// If the caller has returned an error, stop iteration, and return control to them
		if err = fn(*attrs); err != nil {
			return err
		}

		if versions && remote.Deleted.After(remote.Created) {
			err = fn(objval.ObjectAttrs{
				Key:            remote.Name,
				LastModified:   &remote.Deleted,
				IsDeleteMarker: true,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Client) CreateMultipartUpload(_ context.Context, _ objcli.CreateMultipartUploadOptions) (string, error) {
	return uuid.NewString(), nil
}

func (c *Client) ListParts(ctx context.Context, opts objcli.ListPartsOptions) ([]objval.Part, error) {
	var (
		prefix = partPrefix(opts.UploadID, opts.Key)
		parts  = make([]objval.Part, 0)
	)

	fn := func(attrs *objval.ObjectAttrs) error {
		parts = append(parts, objval.Part{
			ID:   attrs.Key,
			Size: ptr.From(attrs.Size),
		})

		return nil
	}

	err := c.IterateObjects(ctx, objcli.IterateObjectsOptions{
		Bucket:    opts.Bucket,
		Prefix:    prefix,
		Delimiter: "/",
		Func:      fn,
	})
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	return parts, nil
}

func (c *Client) UploadPart(ctx context.Context, opts objcli.UploadPartOptions) (objval.Part, error) {
	size, err := objcli.SeekerLength(opts.Body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to determine body length: %w", err)
	}

	intermediate := partKey(opts.UploadID, opts.Key)

	err = c.PutObject(ctx, objcli.PutObjectOptions{
		Bucket:       opts.Bucket,
		Key:          intermediate,
		Body:         opts.Body,
		Precondition: opts.Precondition,
		Lock:         opts.Lock,
	})
	if err != nil {
		return objval.Part{}, err // Purposefully not wrapped
	}

	return objval.Part{ID: intermediate, Number: opts.Number, Size: size}, nil
}

// NOTE: Google storage does not support byte range copying, therefore, only the entire object may be copied; this may
// be done by either not providing a byte range, or providing a byte range for the entire object.
func (c *Client) UploadPartCopy(ctx context.Context, opts objcli.UploadPartCopyOptions) (objval.Part, error) {
	if err := opts.ByteRange.Valid(false); err != nil {
		return objval.Part{}, err // Purposefully not wrapped
	}

	attrs, err := c.GetObjectAttrs(ctx, objcli.GetObjectAttrsOptions{
		Bucket: opts.SourceBucket,
		Key:    opts.SourceKey,
	})
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to get object attributes: %w", err)
	}

	// If the user has provided a byte range, ensure that it's for the entire object
	if opts.ByteRange != nil && !(opts.ByteRange.Start == 0 && opts.ByteRange.End == ptr.From(attrs.Size)-1) {
		return objval.Part{}, objerr.ErrUnsupportedOperation
	}

	var (
		intermediate = partKey(opts.UploadID, opts.DestinationKey)
		srcHandle    = c.serviceAPI.Bucket(opts.SourceBucket).Object(opts.SourceKey)
		dstHandle    = c.serviceAPI.Bucket(opts.DestinationBucket).Object(intermediate)
	)

	// Copying is non-destructive from the source perspective and we don't mind potentially "overwriting" the
	// destination object, always retry.
	_, err = dstHandle.Retryer(storage.WithPolicy(storage.RetryAlways)).CopierFrom(srcHandle).Run(ctx)
	if err != nil {
		return objval.Part{}, handleError(opts.DestinationBucket, intermediate, err)
	}

	return objval.Part{ID: intermediate, Number: opts.Number, Size: ptr.From(attrs.Size)}, nil
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, opts objcli.CompleteMultipartUploadOptions) error {
	converted := make([]string, 0, len(opts.Parts))

	for _, part := range opts.Parts {
		converted = append(converted, part.ID)
	}

	completeOpts := &composeOptions{
		bucket:       opts.Bucket,
		key:          opts.Key,
		parts:        converted,
		precondition: opts.Precondition,
		lock:         opts.Lock,
	}

	err := c.complete(ctx, completeOpts)
	if err != nil {
		return err
	}

	// Object composition may use the source object in the output, ensure that we don't delete it by mistake
	if idx := slices.Index(converted, opts.Key); idx >= 0 {
		converted = slices.Delete(converted, idx, idx+1)
	}

	c.cleanup(ctx, opts.Bucket, converted...)

	return nil
}

func (c *Client) Close() error {
	return c.serviceAPI.Close()
}

// complete recursively composes the object in chunks of 32 eventually resulting in a single complete object.
func (c *Client) complete(ctx context.Context, opts *composeOptions) error {
	if len(opts.parts) <= MaxComposable {
		return c.compose(ctx, opts)
	}

	intermediate := partKey(uuid.NewString(), opts.key)

	defer c.cleanup(ctx, opts.bucket, intermediate)

	intermediateOpts := &composeOptions{
		bucket:       opts.bucket,
		key:          intermediate,
		parts:        opts.parts[:MaxComposable],
		precondition: opts.precondition,
		lock:         opts.lock,
	}

	err := c.compose(ctx, intermediateOpts)
	if err != nil {
		return err
	}

	finalOpts := &composeOptions{
		bucket:       opts.bucket,
		key:          opts.key,
		parts:        append([]string{intermediate}, opts.parts[MaxComposable:]...),
		precondition: opts.precondition,
		lock:         opts.lock,
	}

	return c.complete(ctx, finalOpts)
}

// compose the given parts into a single object.
func (c *Client) compose(ctx context.Context, opts *composeOptions) error {
	handles := make([]objectAPI, 0, len(opts.parts))

	for _, part := range opts.parts {
		handles = append(handles, c.serviceAPI.Bucket(opts.bucket).Object(part))
	}

	// Object composition is non-destructive from the source perspective and we don't mind potentially "overwriting"
	// the destination object, always retry.
	dst := c.serviceAPI.Bucket(opts.bucket).Object(opts.key).Retryer(storage.WithPolicy(storage.RetryAlways))

	if opts.precondition == objcli.OperationPreconditionOnlyIfAbsent {
		dst = dst.If(storage.Conditions{
			DoesNotExist: true,
		})
	}

	composer := dst.ComposerFrom(handles...)

	if opts.lock != nil {
		err := composer.SetLock(opts.lock)
		if err != nil {
			return fmt.Errorf("failed to set object lock: %w", err)
		}
	}

	_, err := composer.Run(ctx)

	return handleError(opts.bucket, opts.key, err)
}

// cleanup attempts to remove the given keys, logging them if we receive an error.
func (c *Client) cleanup(ctx context.Context, bucket string, keys ...string) {
	err := c.DeleteObjects(ctx, objcli.DeleteObjectsOptions{
		Bucket: bucket,
		Keys:   keys,
	})
	if err == nil {
		return
	}

	c.logger.Error("failed to cleanup intermediate keys, they should be removed manually", "keys", keys, "error", err)
}

func (c *Client) AbortMultipartUpload(ctx context.Context, opts objcli.AbortMultipartUploadOptions) error {
	err := c.DeleteDirectory(ctx, objcli.DeleteDirectoryOptions{
		Bucket: opts.Bucket,
		Prefix: partPrefix(opts.UploadID, opts.Key),
	})

	return err
}

func (c *Client) GetBucketLockingStatus(
	ctx context.Context,
	opts objcli.GetBucketLockingStatusOptions,
) (*objval.BucketLockingStatus, error) {
	bucket := c.serviceAPI.Bucket(opts.Bucket)

	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	res := &objval.BucketLockingStatus{
		Enabled: attrs.ObjectRetentionMode != "",
	}

	return res, nil
}

func (c *Client) SetObjectLock(ctx context.Context, opts objcli.SetObjectLockOptions) error {
	if opts.Lock == nil {
		return errors.New("object lock is nil")
	}

	object := c.serviceAPI.Bucket(opts.Bucket).Object(opts.Key)

	if opts.VersionID != "" {
		gen, err := strconv.ParseUint(opts.VersionID, 10, 64)
		if err != nil {
			return handleError(
				opts.Bucket,
				opts.Key,
				fmt.Errorf("failed to parse VersionID into GCP generation: %w", err),
			)
		}

		object = object.Generation(int64(gen))
	}

	switch opts.Lock.Type {
	case objval.LockTypeCompliance:
		_, err := object.Update(ctx, storage.ObjectAttrsToUpdate{
			Retention: &storage.ObjectRetention{
				Mode:        "Locked",
				RetainUntil: opts.Lock.Expiration,
			},
		})
		if err != nil {
			return handleError(opts.Bucket, opts.Key, err)
		}
	default:
		return errors.New("unsupported lock type")
	}

	return nil
}

// getLockType converts GCP's retention mode to 'objval.LockType'.
func getLockType(gcpLockMode string) objval.LockType {
	switch gcpLockMode {
	case "Locked":
		return objval.LockTypeCompliance
	default:
		return objval.LockTypeUndefined
	}
}
