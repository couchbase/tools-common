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
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
	"google.golang.org/api/iterator"

	"github.com/couchbase/tools-common/cloud/v5/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v5/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v5/objstore/objval"
	"github.com/couchbase/tools-common/sync/v2/hofp"
	"github.com/couchbase/tools-common/types/ptr"
	"github.com/couchbase/tools-common/utils/v3/system"
)

// attrs wraps object attributes with internal attributes (e.g. version).
type attrs struct {
	objval.ObjectAttrs
	Version *int64
}

// Client implements the 'objcli.Client' interface allowing the creation/management of objects stored in Google Storage.
type Client struct {
	serviceAPI serviceAPI
	logger     *slog.Logger
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
		serviceAPI: serviceClient{options.Client},
		logger:     options.Logger,
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

	reader, err := c.serviceAPI.Bucket(opts.Bucket).Object(opts.Key).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	remote := reader.Attrs()

	attrs := objval.ObjectAttrs{
		Key:          opts.Key,
		Size:         ptr.To(remote.Size),
		LastModified: ptr.To(remote.LastModified),
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        reader,
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(ctx context.Context, opts objcli.GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	remote, err := c.serviceAPI.Bucket(opts.Bucket).Object(opts.Key).Attrs(ctx)
	if err != nil {
		return nil, handleError(opts.Bucket, opts.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          opts.Key,
		ETag:         ptr.To(remote.Etag),
		Size:         ptr.To(remote.Size),
		LastModified: &remote.Updated,
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
		writer = object.NewWriter(ctx)
	)

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
		srcHdle = c.serviceAPI.Bucket(opts.SourceBucket).Object(opts.SourceKey)
		dstHdle = c.serviceAPI.Bucket(opts.DestinationBucket).Object(opts.DestinationKey)
	)

	// Copying is non-destructive from the source perspective and we don't mind potentially "overwriting" the
	// destination object, always retry.
	_, err := dstHdle.Retryer(storage.WithPolicy(storage.RetryAlways)).CopierFrom(srcHdle).Run(ctx)

	return handleError("", "", err)
}

func (c *Client) AppendToObject(ctx context.Context, opts objcli.AppendToObjectOptions) error {
	attrs, err := c.GetObjectAttrs(ctx, objcli.GetObjectAttrsOptions{
		Bucket: opts.Bucket,
		Key:    opts.Key,
	})

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) || ptr.From(attrs.Size) == 0 {
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
	objects := make([]attrs, 0, len(opts.Keys))

	for _, key := range opts.Keys {
		objects = append(objects, attrs{ObjectAttrs: objval.ObjectAttrs{Key: key}})
	}

	return c.deleteObjects(ctx, opts.Bucket, objects...)
}

// deleteObjects uses a worker pool to delete the given objects.
func (c *Client) deleteObjects(ctx context.Context, bucket string, objects ...attrs) error {
	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(objects)),
	})

	del := func(ctx context.Context, object attrs) error {
		// We correctly handle the case where the object doesn't exist and should have exclusive access to the path
		// prefix in GCP, always retry.
		handle := c.serviceAPI.Bucket(bucket).Object(object.Key).Retryer(storage.WithPolicy(storage.RetryAlways))

		if object.Version != nil {
			handle = handle.Generation(*object.Version)
		}

		err := handle.Delete(ctx)

		if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return handleError(bucket, object.Key, err)
		}

		return nil
	}

	queue := func(object attrs) error {
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
	fn := func(attrs attrs) error {
		return c.deleteObjects(ctx, opts.Bucket, attrs)
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

	return err
}

func (c *Client) IterateObjects(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	fn := func(attrs attrs) error {
		return opts.Func(&attrs.ObjectAttrs)
	}

	return c.iterateObjects(ctx, opts.Bucket, opts.Prefix, opts.Delimiter, false, opts.Include, opts.Exclude, fn)
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
	fn func(attrs) error,
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

	err := query.SetAttrSelection([]string{
		"Name",
		"Etag",
		"Size",
		"Updated",
	})
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
			return fmt.Errorf("failed to get next object: %w", err)
		}

		if objcli.ShouldIgnore(remote.Name, include, exclude) {
			continue
		}

		var (
			key     = remote.Prefix
			size    *int64
			updated *time.Time
			version int64
		)

		// If "key" is empty this isn't a directory stub, treat it as a normal object
		if key == "" {
			key = remote.Name
			size = ptr.To(remote.Size)
			updated = &remote.Updated
			version = remote.Generation
		}

		oa := objval.ObjectAttrs{
			Key:          key,
			Size:         size,
			LastModified: updated,
		}

		attrs := attrs{
			ObjectAttrs: oa,
		}

		// If versions are enabled, populate the attribute
		if versions {
			attrs.Version = ptr.To(version)
		}

		// If the caller has returned an error, stop iteration, and return control to them
		if err = fn(attrs); err != nil {
			return err
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
		Bucket: opts.Bucket,
		Key:    intermediate,
		Body:   opts.Body,
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
		srcHdle      = c.serviceAPI.Bucket(opts.SourceBucket).Object(opts.SourceKey)
		dstHdle      = c.serviceAPI.Bucket(opts.DestinationBucket).Object(intermediate)
	)

	// Copying is non-destructive from the source perspective and we don't mind potentially "overwriting" the
	// destination object, always retry.
	_, err = dstHdle.Retryer(storage.WithPolicy(storage.RetryAlways)).CopierFrom(srcHdle).Run(ctx)
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

	err := c.complete(ctx, opts.Bucket, opts.Key, converted...)
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
func (c *Client) complete(ctx context.Context, bucket, key string, parts ...string) error {
	if len(parts) <= MaxComposable {
		return c.compose(ctx, bucket, key, parts...)
	}

	intermediate := partKey(uuid.NewString(), key)
	defer c.cleanup(ctx, bucket, intermediate)

	err := c.compose(ctx, bucket, intermediate, parts[:MaxComposable]...)
	if err != nil {
		return err
	}

	return c.complete(ctx, bucket, key, append([]string{intermediate}, parts[MaxComposable:]...)...)
}

// compose the given parts into a single object.
func (c *Client) compose(ctx context.Context, bucket, key string, parts ...string) error {
	handles := make([]objectAPI, 0, len(parts))

	for _, part := range parts {
		handles = append(handles, c.serviceAPI.Bucket(bucket).Object(part))
	}

	var (
		// Object composition is non-destructive from the source perspective and we don't mind potentially "overwriting"
		// the destination object, always retry.
		dst    = c.serviceAPI.Bucket(bucket).Object(key).Retryer(storage.WithPolicy(storage.RetryAlways))
		_, err = dst.ComposerFrom(handles...).Run(ctx)
	)

	return handleError(bucket, key, err)
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
