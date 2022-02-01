package objazure

import (
	"context"
	"crypto/md5"
	"encoding/base64"
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

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// NOTE: As apposed to AWS/GCP, Azure use the container/blob naming convention, however, for consistency the Azure
// client implementation continues to use the bucket/key names.

// Client implements the 'objcli.Client' interface allowing the creation/management of blobs stored in Azure blob store.
type Client struct {
	storageAPI blobStorageAPI
}

var _ objcli.Client = (*Client)(nil)

// NewClient returns a new client which uses the given service URL, in general this should be the one created using the
// 'azblob.NewServiceURL' function exposed by the SDK.
func NewClient(url azblob.ServiceURL) *Client {
	return &Client{storageAPI: serviceURL{url: url}}
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

	blobURL := c.storageAPI.ToContainerAPI(bucket).ToBlobAPI(key)

	resp, err := blobURL.Download(
		context.Background(),
		offset,
		length,
		azblob.BlobAccessConditions{},
		false,
		azblob.ClientProvidedKeyOptions{},
	)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	attrs := objval.ObjectAttrs{
		Key:          key,
		Size:         resp.ContentLength(),
		LastModified: aws.Time(resp.LastModified()),
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        resp.Body(azblob.RetryReaderOptions{}),
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(bucket, key string) (*objval.ObjectAttrs, error) {
	blobURL := c.storageAPI.ToContainerAPI(bucket).ToBlobAPI(key)

	resp, err := blobURL.GetProperties(
		context.Background(),
		azblob.BlobAccessConditions{},
		azblob.ClientProvidedKeyOptions{},
	)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          key,
		ETag:         string(resp.ETag()),
		Size:         resp.ContentLength(),
		LastModified: aws.Time(resp.LastModified()),
	}

	return attrs, nil
}

func (c *Client) PutObject(bucket, key string, body io.ReadSeeker) error {
	var (
		md5sum   = md5.New()
		blockURL = c.storageAPI.ToContainerAPI(bucket).ToBlobAPI(key).ToBlockBlobAPI()
	)

	_, err := aws.CopySeekableBody(io.MultiWriter(md5sum), body)
	if err != nil {
		return fmt.Errorf("failed to calculate checksums: %w", err)
	}

	_, err = blockURL.Upload(
		context.Background(),
		body,
		azblob.BlobHTTPHeaders{ContentMD5: md5sum.Sum(nil)},
		azblob.Metadata{},
		azblob.BlobAccessConditions{},
		azblob.AccessTierNone,
		azblob.BlobTagsMap{},
		azblob.ClientProvidedKeyOptions{},
	)

	return handleError(bucket, key, err)
}

func (c *Client) AppendToObject(bucket, key string, data io.ReadSeeker) error {
	attrs, err := c.GetObjectAttrs(bucket, key)

	// As defined by the 'Client' interface, if the given object does not exist, we create it
	if objerr.IsNotFoundError(err) || attrs.Size == 0 {
		return c.PutObject(bucket, key, data)
	}

	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
	}

	id, err := c.CreateMultipartUpload(bucket, key)
	if err != nil {
		return fmt.Errorf("failed to start multipart upload: %w", err)
	}

	intermediate, err := c.UploadPart(bucket, id, key, 2, data)
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	err = c.CompleteMultipartUpload(bucket, id, key, objval.Part{ID: key, Number: 1, Size: attrs.Size}, intermediate)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (c *Client) DeleteObjects(bucket string, keys ...string) error {
	var (
		containerURL = c.storageAPI.ToContainerAPI(bucket)
		pool         = hofp.NewPool(hofp.Options{
			Size:      system.NumWorkers(len(keys)),
			LogPrefix: "(objazure)",
		})
	)

	del := func(key string) error {
		blobURL := containerURL.ToBlobAPI(key)

		_, err := blobURL.Delete(
			context.Background(),
			azblob.DeleteSnapshotsOptionNone,
			azblob.BlobAccessConditions{},
		)
		if err != nil && !isKeyNotFound(err) {
			return handleError(bucket, key, err)
		}

		return nil
	}

	queue := func(key string) error {
		return pool.Queue(func() error { return del(key) })
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
	fn objcli.IterateFunc) error {
	if include != nil && exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	var (
		containerURL = c.storageAPI.ToContainerAPI(bucket)
		options      = azblob.ListBlobsSegmentOptions{Prefix: prefix}
		objects      []*objval.ObjectAttrs
		marker       azblob.Marker
		err          error
	)

	for marker.NotDone() {
		objects, marker, err = c.listBlobs(containerURL, marker, delimiter, options)
		if err != nil {
			return handleError(bucket, "", err)
		}

		err = c.iterateObjects(objects, include, exclude, fn)
		if err != nil {
			return handleError(bucket, "", err)
		}
	}

	return nil
}

// listBlobs lists blobs using either a flat or hierarchical listing depending on whether a delimiter is supplied.
//
// NOTE: The returned marker should be used for the next call to 'listBlobs' to retrieve the next page of items.
func (c *Client) listBlobs(containerURL containerAPI, marker azblob.Marker, delimiter string,
	options azblob.ListBlobsSegmentOptions) ([]*objval.ObjectAttrs, azblob.Marker, error) {
	if delimiter == "" {
		return c.listBlobsFlat(containerURL, marker, options)
	}

	return c.listBlobsHier(containerURL, marker, delimiter, options)
}

// listBlobsFlat performs a flat blob listing returning a page of objects.
func (c *Client) listBlobsFlat(containerURL containerAPI, marker azblob.Marker,
	options azblob.ListBlobsSegmentOptions) ([]*objval.ObjectAttrs, azblob.Marker, error) {
	resp, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, options)
	if err != nil {
		return nil, azblob.Marker{}, err // Purposefully not wrapped
	}

	converted := make([]*objval.ObjectAttrs, 0, len(resp.Segment.BlobItems))

	for _, b := range resp.Segment.BlobItems {
		converted = append(converted, &objval.ObjectAttrs{
			Key:          b.Name,
			Size:         *b.Properties.ContentLength,
			LastModified: &b.Properties.LastModified,
		})
	}

	return converted, resp.NextMarker, nil
}

// listBlobsHier performs a hierarchical blob listing returning a page of directory stubs/blobs.
func (c *Client) listBlobsHier(containerURL containerAPI, marker azblob.Marker, delimiter string,
	options azblob.ListBlobsSegmentOptions) ([]*objval.ObjectAttrs, azblob.Marker, error) {
	resp, err := containerURL.ListBlobsHierarchySegment(context.Background(), marker, delimiter, options)
	if err != nil {
		return nil, azblob.Marker{}, err // Purposefully not wrapped
	}

	converted := make([]*objval.ObjectAttrs, 0, len(resp.Segment.BlobItems))

	for _, p := range resp.Segment.BlobPrefixes {
		converted = append(converted, &objval.ObjectAttrs{Key: p.Name})
	}

	for _, b := range resp.Segment.BlobItems {
		converted = append(converted, &objval.ObjectAttrs{
			Key:          b.Name,
			Size:         *b.Properties.ContentLength,
			LastModified: &b.Properties.LastModified,
		})
	}

	return converted, resp.NextMarker, nil
}

// iterateObjects iterates over the given segment (<=5000) of objects executing the given function for each object which
// has not been explicitly ignored by the user.
func (c *Client) iterateObjects(objects []*objval.ObjectAttrs, include, exclude []*regexp.Regexp,
	fn objcli.IterateFunc) error {
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

	blockURL := c.storageAPI.ToContainerAPI(bucket).ToBlobAPI(key).ToBlockBlobAPI()

	resp, err := blockURL.GetBlockList(
		context.Background(),
		azblob.BlockListUncommitted,
		azblob.LeaseAccessConditions{},
	)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	parts := make([]objval.Part, 0, len(resp.UncommittedBlocks))

	for _, block := range resp.UncommittedBlocks {
		parts = append(parts, objval.Part{ID: block.Name, Size: int64(block.Size)})
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
		md5sum   = md5.New()
		blockID  = base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))
		blockURL = c.storageAPI.ToContainerAPI(bucket).ToBlobAPI(key).ToBlockBlobAPI()
	)

	_, err = aws.CopySeekableBody(md5sum, body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to calculate checksums: %w", err)
	}

	_, err = blockURL.StageBlock(
		context.Background(),
		blockID,
		body,
		azblob.LeaseAccessConditions{},
		md5sum.Sum(nil),
		azblob.ClientProvidedKeyOptions{},
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

	var (
		blockID      = base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))
		containerURL = c.storageAPI.ToContainerAPI(bucket)
		srcURL       = containerURL.ToBlobAPI(src).ToBlockBlobAPI()
		dstURL       = containerURL.ToBlobAPI(dst).ToBlockBlobAPI()
	)

	_, err := dstURL.StageBlockFromURL(
		context.Background(),
		blockID,
		srcURL.URL(),
		offset,
		length,
		azblob.LeaseAccessConditions{},
		azblob.ModifiedAccessConditions{},
		azblob.ClientProvidedKeyOptions{},
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

	var (
		blockURL  = c.storageAPI.ToContainerAPI(bucket).ToBlobAPI(key).ToBlockBlobAPI()
		converted = make([]string, 0, len(parts))
	)

	for _, part := range parts {
		converted = append(converted, part.ID)
	}

	_, err := blockURL.CommitBlockList(
		context.Background(),
		converted,
		azblob.BlobHTTPHeaders{},
		azblob.Metadata{},
		azblob.BlobAccessConditions{},
		azblob.AccessTierNone,
		azblob.BlobTagsMap{},
		azblob.ClientProvidedKeyOptions{},
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
