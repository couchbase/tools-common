package objgcp

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"regexp"
	"strings"

	"github.com/couchbase/tools-common/hofp"
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/slice"
	"github.com/couchbase/tools-common/system"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
)

// Client implements the 'objcli.Client' interface allowing the creation/management of objects stored in Google Storage.
type Client struct {
	serviceAPI serviceAPI
}

var _ objcli.Client = (*Client)(nil)

// NewClient returns a new client which uses the given storage client, in general this should be the one created using
// the 'storage.NewClient' function exposed by the SDK.
func NewClient(client *storage.Client) *Client {
	return &Client{serviceAPI: &serviceClient{client}}
}

func (c *Client) Provider() objval.Provider {
	return objval.ProviderGCP
}

func (c *Client) GetObject(bucket, key string, br *objval.ByteRange) (*objval.Object, error) {
	if err := br.Valid(false); err != nil {
		return nil, err // Purposefully not wrapped
	}

	var offset, length int64 = 0, -1
	if br != nil {
		offset, length = br.ToOffsetLength(length)
	}

	reader, err := c.serviceAPI.Bucket(bucket).Object(key).NewRangeReader(context.Background(), offset, length)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	remote := reader.Attrs()

	attrs := objval.ObjectAttrs{
		Key:          key,
		Size:         remote.Size,
		LastModified: aws.Time(remote.LastModified),
	}

	object := &objval.Object{
		ObjectAttrs: attrs,
		Body:        reader,
	}

	return object, nil
}

func (c *Client) GetObjectAttrs(bucket, key string) (*objval.ObjectAttrs, error) {
	remote, err := c.serviceAPI.Bucket(bucket).Object(key).Attrs(context.Background())
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	attrs := &objval.ObjectAttrs{
		Key:          key,
		ETag:         remote.Etag,
		Size:         remote.Size,
		LastModified: &remote.Updated,
	}

	return attrs, nil
}

func (c *Client) PutObject(bucket, key string, body io.ReadSeeker) error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var (
		md5sum = md5.New()
		crc32c = crc32.New(crc32.MakeTable(crc32.Castagnoli))
		writer = c.serviceAPI.Bucket(bucket).Object(key).NewWriter(ctx)
	)

	_, err := aws.CopySeekableBody(io.MultiWriter(md5sum, crc32c), body)
	if err != nil {
		return fmt.Errorf("failed to calculate checksums: %w", err)
	}

	writer.SendMD5(md5sum.Sum(nil))
	writer.SendCRC(crc32c.Sum32())

	_, err = io.Copy(writer, body)
	if err != nil {
		return handleError(bucket, key, err)
	}

	return handleError(bucket, key, writer.Close())
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
	pool := hofp.NewPool(hofp.Options{
		Size:      system.NumWorkers(len(keys)),
		LogPrefix: "(objgcp)",
	})

	del := func(key string) error {
		err := c.serviceAPI.Bucket(bucket).Object(key).Delete(context.Background())
		if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
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

	return c.IterateObjects(bucket, prefix, nil, nil, fn)
}

func (c *Client) IterateObjects(bucket, prefix string, include, exclude []*regexp.Regexp, fn objcli.IterateFunc) error {
	if include != nil && exclude != nil {
		return objcli.ErrIncludeAndExcludeAreMutuallyExclusive
	}

	it := c.serviceAPI.Bucket(bucket).Objects(context.Background(), &storage.Query{Prefix: prefix})

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

		attrs := &objval.ObjectAttrs{
			Key:          remote.Name,
			ETag:         remote.Etag,
			Size:         remote.Size,
			LastModified: &remote.Updated,
		}

		// If the caller has returned an error, stop iteration, and return control to them
		if err = fn(attrs); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) CreateMultipartUpload(bucket, key string) (string, error) {
	return uuid.NewString(), nil
}

func (c *Client) ListParts(bucket, id, key string) ([]objval.Part, error) {
	var (
		prefix = partPrefix(id, key)
		parts  = make([]objval.Part, 0)
	)

	fn := func(attrs *objval.ObjectAttrs) error {
		parts = append(parts, objval.Part{
			ID:   attrs.Key,
			Size: attrs.Size,
		})

		return nil
	}

	err := c.IterateObjects(bucket, prefix, nil, nil, fn)
	if err != nil {
		return nil, handleError(bucket, key, err)
	}

	return parts, nil
}

func (c *Client) UploadPart(bucket, id, key string, number int, body io.ReadSeeker) (objval.Part, error) {
	size, err := aws.SeekerLen(body)
	if err != nil {
		return objval.Part{}, fmt.Errorf("failed to determine body length: %w", err)
	}

	intermediate := partKey(id, key)

	err = c.PutObject(bucket, intermediate, body)
	if err != nil {
		return objval.Part{}, err // Purposefully not wrapped
	}

	return objval.Part{ID: intermediate, Number: number, Size: size}, nil
}

func (c *Client) CompleteMultipartUpload(bucket, id, key string, parts ...objval.Part) error {
	converted := make([]string, 0, len(parts))

	for _, part := range parts {
		converted = append(converted, part.ID)
	}

	err := c.complete(bucket, key, converted...)
	if err != nil {
		return err
	}

	// Object composition may use the source object in the output, ensure that we don't delete it by mistake
	if idx := slice.FindString(converted, key); idx >= 0 {
		converted, _ = slice.RemoveStringAt(converted, idx)
	}

	c.cleanup(bucket, converted...)

	return nil
}

// complete recursively composes the object in chunks of 32 eventually resulting in a single complete object.
func (c *Client) complete(bucket, key string, parts ...string) error {
	if len(parts) <= MaxComposable {
		return c.compose(bucket, key, parts...)
	}

	intermediate := partKey(uuid.NewString(), key)
	defer c.cleanup(bucket, intermediate)

	err := c.compose(bucket, intermediate, parts[:MaxComposable]...)
	if err != nil {
		return err
	}

	return c.complete(bucket, key, append([]string{intermediate}, parts[MaxComposable:]...)...)
}

// compose the given parts into a single object.
func (c *Client) compose(bucket, key string, parts ...string) error {
	handles := make([]objectAPI, 0, len(parts))

	for _, part := range parts {
		handles = append(handles, c.serviceAPI.Bucket(bucket).Object(part))
	}

	_, err := c.serviceAPI.Bucket(bucket).Object(key).ComposerFrom(handles...).Run(context.Background())

	return handleError(bucket, key, err)
}

// cleanup attempts to remove the given keys, logging them if we receive an error.
func (c *Client) cleanup(bucket string, keys ...string) {
	if err := c.DeleteObjects(bucket, keys...); err == nil {
		return
	}

	log.Errorf(`(Objaws) Failed to cleanup intermediate keys, they should be removed manually | {"keys":[%s]}`,
		strings.Join(keys, ","))
}

func (c *Client) AbortMultipartUpload(bucket, id, key string) error {
	return c.DeleteDirectory(bucket, partPrefix(id, key))
}
