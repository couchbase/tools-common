// Package objaws provides an implementation of 'objstore.Client' for use with AWS S3.
package objaws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"

	"github.com/couchbase/tools-common/cloud/v2/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v2/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v2/objstore/objval"
	"github.com/couchbase/tools-common/core/log"
	"github.com/couchbase/tools-common/sync/hofp"
	"github.com/couchbase/tools-common/types/ptr"
	"github.com/couchbase/tools-common/utils/maths"
	"github.com/couchbase/tools-common/utils/system"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Client implements the 'objcli.Client' interface allowing the creation/management of objects stored in AWS S3.
type Client struct {
	serviceAPI serviceAPI
	logger     log.WrappedLogger
}

var _ objcli.Client = (*Client)(nil)

// ClientOptions encapsulates the options for creating a new AWS Client.
type ClientOptions struct {
	// ServiceAPI is the is the minimal subset of functions that we use from the AWS SDK, this allows for a greatly
	// reduce surface area for mock generation.
	//
	// NOTE: Required
	ServiceAPI serviceAPI
}

// NewClient returns a new client which uses the given 'serviceAPI', in general this should be the one created using the
// 's3.New' function exposed by the SDK.
func NewClient(options ClientOptions) *Client {
	return &Client{serviceAPI: options.ServiceAPI}
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

	if opts.ByteRange != nil {
		input.Range = ptr.To(opts.ByteRange.ToRangeHeader())
	}

	resp, err := c.serviceAPI.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, input.Key, err)
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
	input := &s3.HeadObjectInput{
		Bucket: ptr.To(opts.Bucket),
		Key:    ptr.To(opts.Key),
	}

	resp, err := c.serviceAPI.HeadObjectWithContext(ctx, input)
	if err != nil {
		return nil, handleError(input.Bucket, input.Key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          opts.Key,
		ETag:         resp.ETag,
		Size:         resp.ContentLength,
		LastModified: resp.LastModified,
	}

	return attrs, nil
}

func (c *Client) PutObject(ctx context.Context, opts objcli.PutObjectOptions) error {
	input := &s3.PutObjectInput{
		Body:   opts.Body,
		Bucket: ptr.To(opts.Bucket),
		Key:    ptr.To(opts.Key),
	}

	_, err := c.serviceAPI.PutObjectWithContext(ctx, input)

	return handleError(input.Bucket, input.Key, err)
}

func (c *Client) CopyObject(ctx context.Context, opts objcli.CopyObjectOptions) error {
	input := &s3.CopyObjectInput{
		Bucket:     ptr.To(opts.DestinationBucket),
		Key:        ptr.To(opts.DestinationKey),
		CopySource: ptr.To(url.PathEscape(opts.SourceBucket + "/" + opts.SourceKey)),
	}

	_, err := c.serviceAPI.CopyObjectWithContext(ctx, input)

	return handleError(nil, nil, err)
}

func (c *Client) AppendToObject(ctx context.Context, opts objcli.AppendToObjectOptions) error {
	var (
		bucket = opts.Bucket
		key    = opts.Key
		body   = opts.Body
	)

	attrs, err := c.GetObjectAttrs(ctx, objcli.GetObjectAttrsOptions{Bucket: bucket, Key: key})

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) {
		return c.PutObject(ctx, objcli.PutObjectOptions(opts))
	}

	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
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
) error {
	object, err := c.GetObject(ctx, objcli.GetObjectOptions{Bucket: bucket, Key: attrs.Key})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}

	var buffer bytes.Buffer

	_, err = io.Copy(&buffer, io.MultiReader(object.Body, body))
	if err != nil {
		return fmt.Errorf("failed to download and append to object: %w", err)
	}

	err = c.PutObject(ctx, objcli.PutObjectOptions{
		Bucket: bucket,
		Key:    attrs.Key,
		Body:   bytes.NewReader(buffer.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to upload updated object: %w", err)
	}

	return nil
}

// createMPUThenCopyAndAppend creates a multipart upload, then kicks off the copy and append operation.
func (c *Client) createMPUThenCopyAndAppend(
	ctx context.Context,
	bucket string,
	attrs *objval.ObjectAttrs,
	data io.ReadSeeker,
) error {
	id, err := c.CreateMultipartUpload(ctx, objcli.CreateMultipartUploadOptions{
		Bucket: bucket,
		Key:    attrs.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	err = c.copyAndAppend(ctx, bucket, id, attrs, data)
	if err == nil {
		return nil
	}

	aopts := objcli.AbortMultipartUploadOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      attrs.Key,
	}

	// NOTE: We've failed for some reason, we should try to cleanup after ourselves; the AWS client does not use the
	// given 'parts' argument, so we can omit it here
	if err := c.AbortMultipartUpload(ctx, aopts); err != nil {
		c.logger.Errorf(`(Objaws) Failed to abort multipart upload, it should be aborted manually | `+
			`{"id":"%s","key":"%s"}`, id, attrs.Key)
	}

	return err
}

// copyAndAppend copies all the data from the source object, then uploads a new part and completes the multipart upload.
// This has the affect of appending data to the object, without having to download, and re-upload.
func (c *Client) copyAndAppend(
	ctx context.Context,
	bucket, id string,
	attrs *objval.ObjectAttrs,
	body io.ReadSeeker,
) error {
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
		return fmt.Errorf("failed to copy source object: %w", err)
	}

	appended, err := c.UploadPart(ctx, objcli.UploadPartOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      attrs.Key,
		Number:   2,
		Body:     body,
	})
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	err = c.CompleteMultipartUpload(ctx, objcli.CompleteMultipartUploadOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      attrs.Key,
		Parts:    []objval.Part{copied, appended},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (c *Client) DeleteObjects(ctx context.Context, opts objcli.DeleteObjectsOptions) error {
	pool := hofp.NewPool(hofp.Options{
		Context:   ctx,
		Size:      system.NumWorkers(len(opts.Keys)),
		LogPrefix: "(objaws)",
	})

	del := func(ctx context.Context, start, end int) error {
		return c.deleteObjects(ctx, opts.Bucket, opts.Keys[start:maths.Min(end, len(opts.Keys))]...)
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

func (c *Client) DeleteDirectory(ctx context.Context, opts objcli.DeleteDirectoryOptions) error {
	return c.deleteDirectory(ctx, opts.Bucket, opts.Prefix, c.deleteObjects)
}

// deleteDirectory is a wrapper function which allows unit testing the 'DeleteDirectory' function with a mocked deletion
// callback; this is required because the callback uses 'serviceAPI' which when mocked acquires a lock, causing a
// deadlock.
func (c *Client) deleteDirectory(
	ctx context.Context,
	bucket, prefix string,
	fn func(ctx context.Context, bucket string, keys ...string) error,
) error {
	var err error

	callback := func(page *s3.ListObjectsV2Output, _ bool) bool {
		keys := make([]string, 0, len(page.Contents))

		for _, object := range page.Contents {
			keys = append(keys, *object.Key)
		}

		err = fn(ctx, bucket, keys...)

		return err == nil
	}

	input := &s3.ListObjectsV2Input{
		Bucket: ptr.To(bucket),
		Prefix: ptr.To(prefix),
	}

	// It's important we use an assignment expression here to avoid overwriting the error assigned by our callback
	if err := c.serviceAPI.ListObjectsV2PagesWithContext(ctx, input, callback); err != nil {
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

	input := &s3.DeleteObjectsInput{
		Bucket: ptr.To(bucket),
		Delete: &s3.Delete{Quiet: ptr.To(true)},
	}

	for _, key := range keys {
		input.Delete.Objects = append(input.Delete.Objects, &s3.ObjectIdentifier{Key: ptr.To(key)})
	}

	resp, err := c.serviceAPI.DeleteObjectsWithContext(ctx, input)
	if err != nil {
		return handleError(input.Bucket, nil, err)
	}

	for _, err := range resp.Errors {
		if awsErr := awserr.New(*err.Code, *err.Message, nil); !isKeyNotFound(awsErr) {
			return handleError(input.Bucket, err.Key, awsErr)
		}
	}

	return nil
}

func (c *Client) IterateObjects(ctx context.Context, opts objcli.IterateObjectsOptions) error {
	if opts.Include != nil && opts.Exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	var err error

	callback := func(page *s3.ListObjectsV2Output, _ bool) bool {
		err = c.handlePage(page, opts.Include, opts.Exclude, opts.Func)
		return err == nil
	}

	input := &s3.ListObjectsV2Input{
		Bucket:    ptr.To(opts.Bucket),
		Prefix:    ptr.To(opts.Prefix),
		Delimiter: ptr.To(opts.Delimiter),
	}

	// It's important we use an assignment expression here to avoid overwriting the error assigned by our callback
	if err := c.serviceAPI.ListObjectsV2PagesWithContext(ctx, input, callback); err != nil {
		return handleError(input.Bucket, nil, err)
	}

	return err
}

// handlePage iterates over common prefixes/objects in the given page executing the given function for each object which
// has not been explicitly ignored by the user.
func (c *Client) handlePage(
	page *s3.ListObjectsV2Output,
	include, exclude []*regexp.Regexp,
	fn objcli.IterateFunc,
) error {
	converted := make([]*objval.ObjectAttrs, 0, len(page.CommonPrefixes)+len(page.Contents))

	for _, cp := range page.CommonPrefixes {
		converted = append(converted, &objval.ObjectAttrs{Key: *cp.Prefix})
	}

	for _, o := range page.Contents {
		converted = append(converted, &objval.ObjectAttrs{Key: *o.Key, Size: o.Size, LastModified: o.LastModified})
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

func (c *Client) CreateMultipartUpload(ctx context.Context, opts objcli.CreateMultipartUploadOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: ptr.To(opts.Bucket),
		Key:    ptr.To(opts.Key),
	}

	resp, err := c.serviceAPI.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return "", handleError(input.Bucket, input.Key, err)
	}

	return *resp.UploadId, nil
}

func (c *Client) ListParts(ctx context.Context, opts objcli.ListPartsOptions) ([]objval.Part, error) {
	parts := make([]objval.Part, 0)

	input := &s3.ListPartsInput{
		Bucket:   ptr.To(opts.Bucket),
		UploadId: ptr.To(opts.UploadID),
		Key:      ptr.To(opts.Key),
	}

	err := c.serviceAPI.ListPartsPagesWithContext(ctx, input, func(page *s3.ListPartsOutput, _ bool) bool {
		for _, part := range page.Parts {
			parts = append(parts, objval.Part{ID: *part.ETag, Size: *part.Size})
		}

		return true
	})
	if err == nil {
		return parts, nil
	}

	// Must be handled here localstack may return a clashing "NotFound" error
	if isNoSuchUpload(err) {
		return nil, &objerr.NotFoundError{Type: "upload", Name: opts.UploadID}
	}

	return nil, handleError(input.Bucket, input.Key, err)
}

func (c *Client) UploadPart(ctx context.Context, opts objcli.UploadPartOptions) (objval.Part, error) {
	size, err := aws.SeekerLen(opts.Body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to determine body length: %w", err)
	}

	input := &s3.UploadPartInput{
		Body:          opts.Body,
		Bucket:        ptr.To(opts.Bucket),
		ContentLength: ptr.To(size),
		Key:           ptr.To(opts.Key),
		PartNumber:    ptr.To(int64(opts.Number)),
		UploadId:      ptr.To(opts.UploadID),
	}

	output, err := c.serviceAPI.UploadPartWithContext(ctx, input)
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
		PartNumber:      ptr.To(int64(opts.Number)),
		UploadId:        ptr.To(opts.UploadID),
	}

	output, err := c.serviceAPI.UploadPartCopyWithContext(ctx, input)
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

func (c *Client) CompleteMultipartUpload(ctx context.Context, opts objcli.CompleteMultipartUploadOptions) error {
	converted := make([]*s3.CompletedPart, len(opts.Parts))

	for index, part := range opts.Parts {
		converted[index] = &s3.CompletedPart{ETag: ptr.To(part.ID), PartNumber: ptr.To(int64(part.Number))}
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          ptr.To(opts.Bucket),
		Key:             ptr.To(opts.Key),
		UploadId:        ptr.To(opts.UploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: converted},
	}

	_, err := c.serviceAPI.CompleteMultipartUploadWithContext(ctx, input)

	return handleError(input.Bucket, input.Key, err)
}

func (c *Client) AbortMultipartUpload(ctx context.Context, opts objcli.AbortMultipartUploadOptions) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   ptr.To(opts.Bucket),
		Key:      ptr.To(opts.Key),
		UploadId: ptr.To(opts.UploadID),
	}

	_, err := c.serviceAPI.AbortMultipartUploadWithContext(ctx, input)
	if err != nil && !isNoSuchUpload(err) {
		return handleError(input.Bucket, input.Key, err)
	}

	return nil
}
