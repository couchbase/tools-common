// Package objaws provides an implementation of 'objstore.Client' for use with AWS S3.
package objaws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path"
	"regexp"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
	"github.com/couchbase/tools-common/sync/v2/hofp"
	"github.com/couchbase/tools-common/types/v2/ptr"
	"github.com/couchbase/tools-common/types/v2/timeprovider"
	"github.com/couchbase/tools-common/utils/v3/system"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Client implements the 'objcli.Client' interface allowing the creation/management of objects stored in AWS S3.
type Client struct {
	serviceAPI   serviceAPI
	logger       *slog.Logger
	timeProvider timeprovider.TimeProvider
}

var _ objcli.Client = (*Client)(nil)

// ClientOptions encapsulates the options for creating a new AWS Client.
type ClientOptions struct {
	// ServiceAPI is the is the minimal subset of functions that we use from the AWS SDK, this allows for a greatly
	// reduce surface area for mock generation.
	//
	// NOTE: Required
	ServiceAPI serviceAPI

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

// NewClient returns a new client which uses the given 'serviceAPI', in general this should be the one created using the
// 's3.New' function exposed by the SDK.
func NewClient(options ClientOptions) *Client {
	// Fill out any missing fields with the sane defaults
	options.defaults()

	client := Client{
		serviceAPI:   options.ServiceAPI,
		logger:       options.Logger,
		timeProvider: options.timeProvider,
	}

	if options.timeProvider == nil {
		client.timeProvider = timeprovider.CurrentTimeProvider{}
	}

	return &client
}

func (c *Client) Provider() objval.Provider {
	return objval.ProviderAWS
}

func (c *Client) GetObject(ctx context.Context, opts objcli.GetObjectOptions) (*objval.Object, error) {
	if err := opts.ByteRange.Valid(false); err != nil {
		return nil, err // Purposefully not wrapped
	}

	input := &s3.GetObjectInput{
		Bucket: ptr.To(opts.Bucket),
		Key:    ptr.To(opts.Key),
	}

	if opts.VersionID != "" {
		input.VersionId = ptr.To(opts.VersionID)
	}

	if opts.ByteRange != nil {
		input.Range = ptr.To(opts.ByteRange.ToRangeHeader())
	}

	resp, err := c.serviceAPI.GetObject(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, input.Key, err)
	}

	attrs := objval.ObjectAttrs{
		Key:            opts.Key,
		Size:           resp.ContentLength,
		LastModified:   resp.LastModified,
		LockExpiration: resp.ObjectLockRetainUntilDate,
		LockType:       getLockType(resp.ObjectLockMode),
	}

	if resp.VersionId != nil {
		attrs.VersionID = *resp.VersionId
	}

	if resp.ETag != nil {
		attrs.CAS = *resp.ETag
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        resp.Body,
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(ctx context.Context, opts objcli.GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	input := &s3.HeadObjectInput{
		Bucket: ptr.To(opts.Bucket),
		Key:    ptr.To(opts.Key),
	}

	if opts.VersionID != "" {
		input.VersionId = ptr.To(opts.VersionID)
	}

	resp, err := c.serviceAPI.HeadObject(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, input.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:            opts.Key,
		ETag:           resp.ETag,
		Size:           resp.ContentLength,
		LastModified:   resp.LastModified,
		LockExpiration: resp.ObjectLockRetainUntilDate,
		LockType:       getLockType(resp.ObjectLockMode),
	}

	if resp.VersionId != nil {
		attrs.VersionID = *resp.VersionId
	}

	if resp.ETag != nil {
		attrs.CAS = *resp.ETag
	}

	return attrs, nil
}

func (c *Client) PutObject(ctx context.Context, opts objcli.PutObjectOptions) (*objval.ObjectAttrs, error) {
	input := &s3.PutObjectInput{
		Body:              opts.Body,
		Bucket:            ptr.To(opts.Bucket),
		Key:               ptr.To(opts.Key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	}

	switch opts.Precondition {
	case objcli.OperationPreconditionOnlyIfAbsent:
		input.IfNoneMatch = ptr.To("*")
	case objcli.OperationPreconditionIfMatch:
		input.IfMatch = ptr.To(opts.PreconditionData)
	}

	if opts.Lock != nil {
		switch opts.Lock.Type {
		case objval.LockTypeCompliance:
			input.ObjectLockMode = types.ObjectLockModeCompliance
			input.ObjectLockRetainUntilDate = aws.Time(opts.Lock.Expiration)
		default:
			return nil, errors.New("unsupported lock type")
		}
	}

	output, err := c.serviceAPI.PutObject(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, input.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:  opts.Key,
		ETag: output.ETag,
		Size: output.Size,
	}

	if output.VersionId != nil {
		attrs.VersionID = *output.VersionId
	}

	return attrs, nil
}

func (c *Client) CopyObject(ctx context.Context, opts objcli.CopyObjectOptions) (*objval.ObjectAttrs, error) {
	input := &s3.CopyObjectInput{
		Bucket:     ptr.To(opts.DestinationBucket),
		Key:        ptr.To(opts.DestinationKey),
		CopySource: ptr.To(url.PathEscape(opts.SourceBucket + "/" + opts.SourceKey)),
	}

	output, err := c.serviceAPI.CopyObject(ctx, input)
	if err != nil {
		return nil, handleError(nil, nil, err)
	}

	attrs := &objval.ObjectAttrs{
		Key: opts.DestinationKey,
	}

	if output.CopyObjectResult != nil {
		attrs.ETag = output.CopyObjectResult.ETag
		attrs.LastModified = output.CopyObjectResult.LastModified
	}

	if output.VersionId != nil {
		attrs.VersionID = *output.VersionId
	}

	return attrs, nil
}

func (c *Client) AppendToObject(ctx context.Context, opts objcli.AppendToObjectOptions) (*objval.ObjectAttrs, error) {
	var (
		bucket = opts.Bucket
		key    = opts.Key
		body   = opts.Body
	)

	attrs, err := c.GetObjectAttrs(ctx, objcli.GetObjectAttrsOptions{Bucket: bucket, Key: key})

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) {
		putOpts := objcli.PutObjectOptions{
			Bucket: opts.Bucket,
			Key:    opts.Key,
			Body:   opts.Body,
		}

		attrs, err := c.PutObject(ctx, putOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to put object: %w", err)
		}

		return attrs, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get object attributes: %w", err)
	}

	if ptr.From(attrs.Size) < MinUploadSize {
		return c.downloadAndAppend(ctx, bucket, attrs, body)
	}

	return c.createMPUThenCopyAndAppend(ctx, bucket, attrs, body)
}

// downloadAndAppend downloads an object, and appends the given data to it before uploading it back to S3; this should
// be used for objects which are less than 5MiB in size (i.e. under the multipart upload minium size).
func (c *Client) downloadAndAppend(
	ctx context.Context,
	bucket string,
	attrs *objval.ObjectAttrs,
	body io.ReadSeeker,
) (*objval.ObjectAttrs, error) {
	object, err := c.GetObject(ctx, objcli.GetObjectOptions{Bucket: bucket, Key: attrs.Key})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}

	var buffer bytes.Buffer

	_, err = io.Copy(&buffer, io.MultiReader(object.Body, body))
	if err != nil {
		return nil, fmt.Errorf("failed to download and append to object: %w", err)
	}

	outputAttrs, err := c.PutObject(ctx, objcli.PutObjectOptions{
		Bucket: bucket,
		Key:    attrs.Key,
		Body:   bytes.NewReader(buffer.Bytes()),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upload updated object: %w", err)
	}

	return outputAttrs, nil
}

// createMPUThenCopyAndAppend creates a multipart upload, then kicks off the copy and append operation.
func (c *Client) createMPUThenCopyAndAppend(
	ctx context.Context,
	bucket string,
	attrs *objval.ObjectAttrs,
	data io.ReadSeeker,
) (*objval.ObjectAttrs, error) {
	id, err := c.CreateMultipartUpload(ctx, objcli.CreateMultipartUploadOptions{
		Bucket: bucket,
		Key:    attrs.Key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	outputAttrs, err := c.copyAndAppend(ctx, bucket, id, attrs, data)
	if err == nil {
		return outputAttrs, nil
	}

	aOpts := objcli.AbortMultipartUploadOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      attrs.Key,
	}

	// NOTE: We've failed for some reason, we should try to cleanup after ourselves; the AWS client does not use the
	// given 'parts' argument, so we can omit it here
	if err := c.AbortMultipartUpload(ctx, aOpts); err != nil {
		c.logger.Error("failed to abort multipart upload, it should be aborted manually", "id", id, "key", attrs.Key)
	}

	return nil, err
}

// copyAndAppend copies all the data from the source object, then uploads a new part and completes the multipart upload.
// This has the affect of appending data to the object, without having to download, and re-upload.
func (c *Client) copyAndAppend(
	ctx context.Context,
	bucket, id string,
	attrs *objval.ObjectAttrs,
	body io.ReadSeeker,
) (*objval.ObjectAttrs, error) {
	copied, err := c.UploadPartCopy(ctx, objcli.UploadPartCopyOptions{
		DestinationBucket: bucket,
		UploadID:          id,
		DestinationKey:    attrs.Key,
		SourceBucket:      bucket,
		SourceKey:         attrs.Key,
		Number:            1,
		// The attributes uses here are obtained from 'GetObjectAttrs' so the 'Size' will be non-nil.
		ByteRange: &objval.ByteRange{End: ptr.From(attrs.Size) - 1},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to copy source object: %w", err)
	}

	appended, err := c.UploadPart(ctx, objcli.UploadPartOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      attrs.Key,
		Number:   2,
		Body:     body,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upload part: %w", err)
	}

	outputAttrs, err := c.CompleteMultipartUpload(ctx, objcli.CompleteMultipartUploadOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      attrs.Key,
		Parts:    []objval.Part{copied, appended},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return outputAttrs, nil
}

func (c *Client) DeleteObjects(ctx context.Context, opts objcli.DeleteObjectsOptions) error {
	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(opts.Keys)),
	})

	del := func(ctx context.Context, start, end int) error {
		return c.deleteObjects(ctx, opts.Bucket, opts.Keys[start:min(end, len(opts.Keys))]...)
	}

	queue := func(start, end int) error {
		return pool.Queue(func(ctx context.Context) error { return del(ctx, start, end) })
	}

	for start, end := 0, PageSize; start < len(opts.Keys); start, end = start+PageSize, end+PageSize {
		if queue(start, end) != nil {
			break
		}
	}

	return pool.Stop()
}

func (c *Client) DeleteObjectVersions(ctx context.Context, opts objcli.DeleteObjectVersionsOptions) error {
	versions := make([]types.ObjectIdentifier, 0)

	for _, objVersion := range opts.Versions {
		versions = append(versions, types.ObjectIdentifier{
			Key:       &objVersion.Key,
			VersionId: &objVersion.VersionID,
		})
	}

	pool := hofp.NewPool(hofp.Options{
		Context: ctx,
		Size:    system.NumWorkers(len(versions)),
	})

	del := func(ctx context.Context, start, end int) error {
		return c.deleteObjectVersions(ctx, opts.Bucket, versions[start:min(end, len(versions))]...)
	}

	queue := func(start, end int) error {
		return pool.Queue(func(ctx context.Context) error { return del(ctx, start, end) })
	}

	for start, end := 0, PageSize; start < len(versions); start, end = start+PageSize, end+PageSize {
		if queue(start, end) != nil {
			break
		}
	}

	return pool.Stop()
}

// DeleteDirectory deletes all objects in a specific directory of a bucket. This does not delete old versions of objects
// if any.
func (c *Client) DeleteDirectory(ctx context.Context, opts objcli.DeleteDirectoryOptions) error {
	if opts.Versions {
		return c.deleteDirectoryVersions(ctx, opts.Bucket, opts.Prefix, c.deleteObjectVersions)
	}

	return c.deleteDirectory(ctx, opts.Bucket, opts.Prefix, c.deleteObjects)
}

// Close is a no-op for AWS as this won't result in a memory leak.
func (c *Client) Close() error {
	return nil
}

// deleteDirectory is a wrapper function which allows unit testing the 'DeleteDirectory' function with a mocked deletion
// callback; this is required because the callback uses 'serviceAPI' which when mocked acquires a lock, causing a
// deadlock.
func (c *Client) deleteDirectory(
	ctx context.Context,
	bucket, prefix string,
	fn func(ctx context.Context, bucket string, keys ...string) error,
) error {
	callback := func(page *s3.ListObjectsV2Output) error {
		keys := make([]string, 0, len(page.Contents))

		for _, object := range page.Contents {
			keys = append(keys, *object.Key)
		}

		return fn(ctx, bucket, keys...)
	}

	input := &s3.ListObjectsV2Input{
		Bucket: ptr.To(bucket),
		Prefix: ptr.To(prefix),
	}

	err := c.listObjects(ctx, input, callback)
	if err != nil {
		return handleError(input.Bucket, nil, err)
	}

	return err
}

// deleteDirectoryVersions is a wrapper function which allows unit testing the 'DeleteDirectory' function with a mocked
// deletion callback; this is required because the callback uses 'serviceAPI' which when mocked acquires a lock,
// causing a deadlock.
func (c *Client) deleteDirectoryVersions(
	ctx context.Context,
	bucket, prefix string,
	fn func(ctx context.Context, bucket string, objects ...types.ObjectIdentifier) error,
) error {
	callback := func(page *s3.ListObjectVersionsOutput) error {
		objects := make([]types.ObjectIdentifier, 0, len(page.Versions)+len(page.DeleteMarkers))

		for _, object := range page.Versions {
			objects = append(objects, types.ObjectIdentifier{
				Key:       object.Key,
				VersionId: object.VersionId,
			})
		}

		for _, object := range page.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{
				Key:       object.Key,
				VersionId: object.VersionId,
			})
		}

		return fn(ctx, bucket, objects...)
	}

	input := &s3.ListObjectVersionsInput{
		Bucket: ptr.To(bucket),
		Prefix: ptr.To(prefix),
	}

	err := c.listObjectVersions(ctx, input, callback)
	if err != nil {
		return handleError(input.Bucket, nil, err)
	}

	return err
}

// deleteObjects performs a batched delete operation for a single page (<=1000) of keys.
func (c *Client) deleteObjects(ctx context.Context, bucket string, keys ...string) error {
	// We use ListObjectsV2PagesWithContext to delete a directory. This takes deleteObjects
	// as a callback whenever it receives a page, even if that page is empty. We then call
	// DeleteObjectsWithContext even if there aren't any keys. If no keys are found we return
	// early to avoid returning the error
	if len(keys) == 0 {
		return nil
	}

	objectIdentifiers := make([]types.ObjectIdentifier, 0, len(keys))

	for _, key := range keys {
		objectIdentifiers = append(objectIdentifiers, types.ObjectIdentifier{Key: ptr.To(key)})
	}

	return c.deleteObjectVersions(ctx, bucket, objectIdentifiers...)
}

// deleteObjectVersions performs a batched delete operation for a single page (<=1000) of object versions.
func (c *Client) deleteObjectVersions(ctx context.Context, bucket string, objects ...types.ObjectIdentifier) error {
	if len(objects) == 0 {
		return nil
	}

	input := &s3.DeleteObjectsInput{
		Bucket: ptr.To(bucket),
		Delete: &types.Delete{
			Quiet:   ptr.To(true),
			Objects: objects,
		},
	}

	resp, err := c.serviceAPI.DeleteObjects(ctx, input)
	if err != nil {
		return handleError(input.Bucket, nil, err)
	}

	for _, err := range resp.Errors {
		converted := &smithy.GenericAPIError{
			Code:    *err.Code,
			Message: *err.Message,
		}

		if isKeyNotFound(converted) {
			continue
		}

		return handleError(input.Bucket, err.Key, converted)
	}

	return nil
}

func (c *Client) IterateObjects(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	if opts.Include != nil && opts.Exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	if opts.Versions {
		err := c.iterateObjectVersions(ctx, opts)
		if err != nil {
			return fmt.Errorf("failed to iterate object versions: %w", err)
		}

		return nil
	}

	return c.iterateObjects(ctx, opts)
}

func (c *Client) iterateObjectVersions(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	callback := func(pageOutput *s3.ListObjectVersionsOutput) error {
		page := listObjectsOutput{
			commonPrefixes: pageOutput.CommonPrefixes,
			versions:       pageOutput.Versions,
			deleteMarkers:  pageOutput.DeleteMarkers,
		}

		return c.handlePage(page, opts.Include, opts.Exclude, opts.Func)
	}

	input := &s3.ListObjectVersionsInput{
		Bucket:    ptr.To(opts.Bucket),
		Prefix:    ptr.To(opts.Prefix),
		Delimiter: ptr.To(opts.Delimiter),
	}

	err := c.listObjectVersions(ctx, input, callback)
	if err != nil {
		return handleError(input.Bucket, nil, err)
	}

	return nil
}

func (c *Client) iterateObjects(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	callback := func(pageOutput *s3.ListObjectsV2Output) error {
		page := listObjectsOutput{
			commonPrefixes: pageOutput.CommonPrefixes,
			contents:       pageOutput.Contents,
		}

		return c.handlePage(page, opts.Include, opts.Exclude, opts.Func)
	}

	input := &s3.ListObjectsV2Input{
		Bucket:    ptr.To(opts.Bucket),
		Prefix:    ptr.To(opts.Prefix),
		Delimiter: ptr.To(opts.Delimiter),
	}

	err := c.listObjects(ctx, input, callback)
	if err != nil {
		return handleError(input.Bucket, nil, err)
	}

	return nil
}

// handlePage iterates over common prefixes/objects in the given page executing the given function for each object which
// has not been explicitly ignored by the user.
func (c *Client) handlePage(
	page listObjectsOutput,
	include, exclude []*regexp.Regexp,
	fn objcli.IterateFunc,
) error {
	converted := make([]*objval.ObjectAttrs, 0, len(page.commonPrefixes)+len(page.contents))

	for _, cp := range page.commonPrefixes {
		converted = append(converted, &objval.ObjectAttrs{Key: *cp.Prefix})
	}

	for _, o := range page.contents {
		converted = append(converted, &objval.ObjectAttrs{Key: *o.Key, Size: o.Size, LastModified: o.LastModified})
	}

	for _, ov := range page.versions {
		obj := &objval.ObjectAttrs{
			Key:          *ov.Key,
			Size:         ov.Size,
			LastModified: ov.LastModified,
		}

		if ov.VersionId != nil {
			obj.VersionID = *ov.VersionId
		}

		if ov.IsLatest != nil {
			obj.IsCurrentVersion = *ov.IsLatest
		}

		converted = append(converted, obj)
	}

	for _, dm := range page.deleteMarkers {
		obj := &objval.ObjectAttrs{Key: *dm.Key, IsDeleteMarker: true, LastModified: dm.LastModified}

		if dm.VersionId != nil {
			obj.VersionID = *dm.VersionId
		}

		if dm.IsLatest != nil {
			obj.IsCurrentVersion = *dm.IsLatest
		}

		converted = append(converted, obj)
	}

	for _, attrs := range converted {
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

// listObjects uses the SDK paginator to run the given function on pages of objects.
func (c *Client) listObjects(
	ctx context.Context,
	input *s3.ListObjectsV2Input,
	fn func(page *s3.ListObjectsV2Output) error,
) error {
	return listObjects(ctx, s3.NewListObjectsV2Paginator(c.serviceAPI, input), fn)
}

// listObjectVersions uses the SDK paginator to run the given function on pages of object versions.
func (c *Client) listObjectVersions(
	ctx context.Context,
	input *s3.ListObjectVersionsInput,
	fn func(page *s3.ListObjectVersionsOutput) error,
) error {
	return listObjects(ctx, s3.NewListObjectVersionsPaginator(c.serviceAPI, input), fn)
}

func (c *Client) CreateMultipartUpload(ctx context.Context, opts objcli.CreateMultipartUploadOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: ptr.To(opts.Bucket),
		Key:    ptr.To(opts.Key),
	}

	if opts.Lock != nil {
		switch opts.Lock.Type {
		case objval.LockTypeCompliance:
			input.ObjectLockMode = types.ObjectLockModeCompliance
			input.ObjectLockRetainUntilDate = aws.Time(opts.Lock.Expiration)
		default:
			return "", errors.New("unsupported lock type")
		}
	}

	resp, err := c.serviceAPI.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", handleError(input.Bucket, input.Key, err)
	}

	return *resp.UploadId, nil
}

func (c *Client) ListParts(ctx context.Context, opts objcli.ListPartsOptions) ([]objval.Part, error) {
	input := &s3.ListPartsInput{
		Bucket:   ptr.To(opts.Bucket),
		UploadId: ptr.To(opts.UploadID),
		Key:      ptr.To(opts.Key),
	}

	parts, err := c.listParts(
		ctx,
		s3.NewListPartsPaginator(c.serviceAPI, input),
	)
	if err == nil {
		return parts, nil
	}

	// Must be handled here localstack may return a clashing "NotFound" error
	if isNoSuchUpload(err) {
		return nil, &objerr.NotFoundError{Type: "upload", Name: opts.UploadID}
	}

	return nil, handleError(input.Bucket, input.Key, err)
}

// listParts uses the SDK paginator to run the given function on pages of parts.
func (c *Client) listParts(ctx context.Context, paginator *s3.ListPartsPaginator) ([]objval.Part, error) {
	parts := make([]objval.Part, 0)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page: %w", err)
		}

		for _, part := range page.Parts {
			parts = append(parts, objval.Part{ID: *part.ETag, Size: *part.Size})
		}
	}

	return parts, nil
}

func (c *Client) UploadPart(ctx context.Context, opts objcli.UploadPartOptions) (objval.Part, error) {
	size, err := objcli.SeekerLength(opts.Body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to determine body length: %w", err)
	}

	input := &s3.UploadPartInput{
		Body:          opts.Body,
		Bucket:        ptr.To(opts.Bucket),
		ContentLength: ptr.To(size),
		Key:           ptr.To(opts.Key),
		PartNumber:    ptr.To(int32(opts.Number)),
		UploadId:      ptr.To(opts.UploadID),
		// We only specify the checksum for the upload of parts as AWS supports combining the CRC32 checksums to find the full
		// object checksum. Furthermore the SDK doesn't appear to calculate the full checksum for us.
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	}

	output, err := c.serviceAPI.UploadPart(ctx, input)
	if err != nil {
		return objval.Part{}, handleError(input.Bucket, input.Key, err)
	}

	return objval.Part{ID: *output.ETag, Number: opts.Number, Size: size}, nil
}

func (c *Client) UploadPartCopy(ctx context.Context, opts objcli.UploadPartCopyOptions) (objval.Part, error) {
	if err := opts.ByteRange.Valid(true); err != nil {
		return objval.Part{}, err // Purposefully not wrapped
	}

	input := &s3.UploadPartCopyInput{
		Bucket:          ptr.To(opts.DestinationBucket),
		CopySource:      ptr.To(path.Join(opts.SourceBucket, opts.SourceKey)),
		CopySourceRange: ptr.To(opts.ByteRange.ToRangeHeader()),
		Key:             ptr.To(opts.DestinationKey),
		PartNumber:      ptr.To(int32(opts.Number)),
		UploadId:        ptr.To(opts.UploadID),
	}

	output, err := c.serviceAPI.UploadPartCopy(ctx, input)
	if err != nil {
		return objval.Part{}, handleError(input.Bucket, input.Key, err)
	}

	part := objval.Part{
		ID:     *output.CopyPartResult.ETag,
		Number: opts.Number,
		Size:   opts.ByteRange.End - opts.ByteRange.Start + 1,
	}

	return part, nil
}

func (c *Client) CompleteMultipartUpload(
	ctx context.Context, opts objcli.CompleteMultipartUploadOptions,
) (*objval.ObjectAttrs, error) {
	converted := make([]types.CompletedPart, len(opts.Parts))

	for index, part := range opts.Parts {
		converted[index] = types.CompletedPart{ETag: ptr.To(part.ID), PartNumber: ptr.To(int32(part.Number))}
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          ptr.To(opts.Bucket),
		Key:             ptr.To(opts.Key),
		UploadId:        ptr.To(opts.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: converted},
	}

	if opts.Precondition == objcli.OperationPreconditionOnlyIfAbsent {
		input.IfNoneMatch = ptr.To("*")
	}

	output, err := c.serviceAPI.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, input.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:  opts.Key,
		ETag: output.ETag,
	}

	if output.VersionId != nil {
		attrs.VersionID = *output.VersionId
	}

	return attrs, nil
}

func (c *Client) AbortMultipartUpload(ctx context.Context, opts objcli.AbortMultipartUploadOptions) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   ptr.To(opts.Bucket),
		Key:      ptr.To(opts.Key),
		UploadId: ptr.To(opts.UploadID),
	}

	_, err := c.serviceAPI.AbortMultipartUpload(ctx, input)
	if err != nil && !isNoSuchUpload(err) {
		return handleError(input.Bucket, input.Key, err)
	}

	return nil
}

func (c *Client) GetBucketVersioningStatus(
	ctx context.Context,
	opts objcli.GetBucketLockingStatusOptions,
) (*objval.BucketVersioningStatus, error) {
	input := &s3.GetBucketVersioningInput{
		Bucket: ptr.To(opts.Bucket),
	}

	result := &objval.BucketVersioningStatus{}

	output, err := c.serviceAPI.GetBucketVersioning(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, nil, err)
	}

	result.Enabled = output.Status == types.BucketVersioningStatusEnabled

	return result, nil
}

func (c *Client) GetBucketLockingStatus(
	ctx context.Context,
	opts objcli.GetBucketLockingStatusOptions,
) (*objval.BucketLockingStatus, error) {
	input := &s3.GetObjectLockConfigurationInput{
		Bucket: ptr.To(opts.Bucket),
	}

	result := &objval.BucketLockingStatus{}

	output, err := c.serviceAPI.GetObjectLockConfiguration(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ObjectLockConfigurationNotFoundError" {
			return result, nil
		}

		return nil, handleError(input.Bucket, nil, err)
	}

	result.Enabled = output.ObjectLockConfiguration.ObjectLockEnabled == types.ObjectLockEnabledEnabled

	return result, nil
}

func (c *Client) SetObjectLock(ctx context.Context, opts objcli.SetObjectLockOptions) error {
	if opts.Lock == nil {
		return errors.New("object lock is nil")
	}

	input := &s3.PutObjectRetentionInput{
		Bucket:    ptr.To(opts.Bucket),
		Key:       ptr.To(opts.Key),
		VersionId: ptr.To(opts.VersionID),
	}

	switch opts.Lock.Type {
	case objval.LockTypeCompliance:
		input.Retention = &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeCompliance,
			RetainUntilDate: aws.Time(opts.Lock.Expiration),
		}
	default:
		return errors.New("unsupported lock type")
	}

	_, err := c.serviceAPI.PutObjectRetention(ctx, input)
	if err != nil {
		return handleError(input.Bucket, input.Key, err)
	}

	return nil
}

// paginator wraps the AWS paginator API in an interface.
type paginator[T any] interface {
	HasMorePages() bool
	NextPage(context.Context, ...func(*s3.Options)) (T, error)
}

// listObjects runs the given function for each page in the paginator.
func listObjects[O any](ctx context.Context, pgn paginator[O], fn func(O) error) error {
	for pgn.HasMorePages() {
		page, err := pgn.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		err = fn(page)
		if err != nil {
			return fmt.Errorf("failed to process page: %w", err)
		}
	}

	return nil
}

// getLockType converts S3's 'types.ObjectLockMode' to 'objval.LockType'.
func getLockType(awsLockMode types.ObjectLockMode) objval.LockType {
	switch awsLockMode {
	case types.ObjectLockModeCompliance:
		return objval.LockTypeCompliance
	default:
		// Currently, we only care if the object is in 'compliance' mode.
		return objval.LockTypeUndefined
	}
}
