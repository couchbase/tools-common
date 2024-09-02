package objcli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/couchbase/tools-common/cloud/v6/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v6/objstore/objval"
	testutil "github.com/couchbase/tools-common/testing/util"
	"github.com/couchbase/tools-common/types/ptr"
)

// TestClient implementation of the 'Client' interface which stores state in memory, and can be used to avoid having to
// manually mock a client during unit testing.
type TestClient struct {
	t        *testing.T
	lock     sync.RWMutex
	provider objval.Provider

	// Buckets is the in memory state maintained by the client. Internally, access is guarded by a mutex, however, it's
	// not safe/recommended to access this attribute whilst a test is running; it should only be used to inspect state
	// (to perform assertions) once testing is complete.
	Buckets objval.TestBuckets
}

var _ Client = (*TestClient)(nil)

// NewTestClient returns a new test client, which has no buckets/objects.
func NewTestClient(t *testing.T, provider objval.Provider) *TestClient {
	return &TestClient{
		t:        t,
		provider: provider,
		Buckets:  make(objval.TestBuckets),
	}
}

func (t *TestClient) Provider() objval.Provider {
	return t.provider
}

func (t *TestClient) GetObject(_ context.Context, opts GetObjectOptions) (*objval.Object, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	object, err := t.getObjectRLocked(opts.Bucket, opts.Key)
	if err != nil {
		return nil, err
	}

	var offset, length int64 = 0, int64(len(object.Body) + 1)
	if opts.ByteRange != nil {
		offset, length = opts.ByteRange.ToOffsetLength(length)
	}

	return &objval.Object{
		ObjectAttrs: object.ObjectAttrs,
		Body:        io.NopCloser(io.NewSectionReader(bytes.NewReader(object.Body), offset, length)),
	}, nil
}

func (t *TestClient) GetObjectAttrs(_ context.Context, opts GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	object, err := t.getObjectRLocked(opts.Bucket, opts.Key)
	if err != nil {
		return nil, err
	}

	return &object.ObjectAttrs, nil
}

func (t *TestClient) PutObject(_ context.Context, opts PutObjectOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_ = t.putObjectLocked(opts.Bucket, opts.Key, opts.Body)

	return nil
}

func (t *TestClient) CopyObject(_ context.Context, opts CopyObjectOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	src, err := t.getObjectRLocked(opts.SourceBucket, opts.SourceKey)
	if err != nil {
		return err
	}

	_ = t.putObjectLocked(opts.DestinationBucket, opts.DestinationKey, bytes.NewReader(src.Body))

	return nil
}

func (t *TestClient) AppendToObject(_ context.Context, opts AppendToObjectOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, ok := t.getBucketLocked(opts.Bucket)[opts.Key]
	if ok {
		object.Body = append(object.Body, testutil.ReadAll(t.t, opts.Body)...)
	} else {
		_ = t.putObjectLocked(opts.Bucket, opts.Key, opts.Body)
	}

	return nil
}

func (t *TestClient) DeleteObjects(_ context.Context, opts DeleteObjectsOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(opts.Bucket)

	for _, key := range opts.Keys {
		delete(b, key)
	}

	return nil
}

func (t *TestClient) DeleteDirectory(_ context.Context, opts DeleteDirectoryOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(opts.Bucket)

	for key := range b {
		if !strings.HasPrefix(key, opts.Prefix) {
			continue
		}

		delete(b, key)
	}

	return nil
}

func (t *TestClient) IterateObjects(_ context.Context, opts IterateObjectsOptions) error {
	if opts.Include != nil && opts.Exclude != nil {
		return ErrIncludeAndExcludeAreMutuallyExclusive
	}

	t.lock.RLock()

	b, ok := t.Buckets[opts.Bucket]
	if !ok {
		t.lock.RUnlock()
		return nil
	}

	// Take a copy of the bucket. This stops a deadlock that happens if fn is trying to perform an operation which
	// requires a write lock
	cpy := maps.Clone(b)

	t.lock.RUnlock()

	seen := make(map[string]struct{})

	for key, object := range cpy {
		if !strings.HasPrefix(key, opts.Prefix) || ShouldIgnore(key, opts.Include, opts.Exclude) {
			continue
		}

		var (
			attrs   = object.ObjectAttrs
			trimmed = strings.TrimPrefix(key, opts.Prefix)
		)

		// If this is a nested key, convert it into a directory stub. AWS allows a filesystem style API when you pass a
		// delimiter - if your prefix has a "directory" in it we get a stub, rather than the actual object which could
		// be nested.
		if opts.Delimiter != "" && strings.Count(trimmed, opts.Delimiter) > 1 {
			attrs.Key = parentDirectory(key)
			attrs.ETag = nil
			attrs.Size = nil
			attrs.LastModified = nil
		}

		// Don't return duplicate values
		if _, ok := seen[attrs.Key]; ok {
			continue
		}

		seen[attrs.Key] = struct{}{}

		if err := opts.Func(&attrs); err != nil {
			return err
		}
	}

	return nil
}

func (t *TestClient) Close() error {
	return nil
}

func (t *TestClient) CreateMultipartUpload(_ context.Context, _ CreateMultipartUploadOptions) (string, error) {
	return uuid.NewString(), nil
}

func (t *TestClient) ListParts(ctx context.Context, opts ListPartsOptions) ([]objval.Part, error) {
	var (
		prefix = partPrefix(opts.UploadID, opts.Key)
		parts  = make([]objval.Part, 0)
	)

	fn := func(attrs *objval.ObjectAttrs) error {
		if strings.HasPrefix(attrs.Key, prefix) {
			parts = append(parts, objval.Part{ID: attrs.Key, Size: ptr.From(attrs.Size)})
		}

		return nil
	}

	err := t.IterateObjects(ctx, IterateObjectsOptions{
		Bucket:    opts.Bucket,
		Prefix:    prefix,
		Delimiter: "/",
		Func:      fn,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to iterate objects: %w", err)
	}

	return parts, nil
}

func (t *TestClient) UploadPart(_ context.Context, opts UploadPartOptions) (objval.Part, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	size, err := SeekerLength(opts.Body)
	require.NoError(t.t, err)

	part := objval.Part{
		ID:     t.putObjectLocked(opts.Bucket, partKey(opts.UploadID, opts.Key), opts.Body),
		Number: opts.Number,
		Size:   size,
	}

	return part, nil
}

func (t *TestClient) UploadPartCopy(_ context.Context, opts UploadPartCopyOptions) (objval.Part, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, err := t.getObjectRLocked(opts.SourceBucket, opts.SourceKey)
	if err != nil {
		return objval.Part{}, err
	}

	body := make([]byte, opts.ByteRange.End-opts.ByteRange.Start+1)
	copy(body, object.Body)

	id := t.putObjectLocked(
		opts.DestinationBucket,
		partKey(opts.UploadID, opts.DestinationKey),
		bytes.NewReader(body),
	)

	part := objval.Part{
		ID:     id,
		Number: opts.Number,
		Size:   int64(len(body)),
	}

	return part, nil
}

func (t *TestClient) CompleteMultipartUpload(_ context.Context, opts CompleteMultipartUploadOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	buffer := &bytes.Buffer{}

	for _, part := range opts.Parts {
		object, err := t.getObjectRLocked(opts.Bucket, part.ID)
		if err != nil {
			return err
		}

		buffer.Write(object.Body)
	}

	_ = t.putObjectLocked(opts.Bucket, opts.Key, bytes.NewReader(buffer.Bytes()))

	t.deleteKeysLocked(opts.Bucket, partPrefix(opts.UploadID, opts.Key), nil, nil)

	return nil
}

func (t *TestClient) AbortMultipartUpload(_ context.Context, opts AbortMultipartUploadOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.deleteKeysLocked(opts.Bucket, partPrefix(opts.UploadID, opts.Key), nil, nil)

	return nil
}

func (t *TestClient) getBucketLocked(bucket string) objval.TestBucket {
	_, ok := t.Buckets[bucket]
	if !ok {
		t.Buckets[bucket] = make(objval.TestBucket)
	}

	return t.Buckets[bucket]
}

// NOTE: Buckets are automatically created by the test client when they're required, so this error returns an object not
// found error if either the bucket/object don't exist.
func (t *TestClient) getObjectRLocked(bucket, key string) (*objval.TestObject, error) {
	b, ok := t.Buckets[bucket]
	if !ok {
		return nil, &objerr.NotFoundError{Type: "object", Name: key}
	}

	o, ok := b[key]
	if !ok {
		return nil, &objerr.NotFoundError{Type: "object", Name: key}
	}

	return o, nil
}

func (t *TestClient) putObjectLocked(bucket, key string, body io.ReadSeeker) string {
	var (
		now  = time.Now()
		data = testutil.ReadAll(t.t, body)
	)

	attrs := objval.ObjectAttrs{
		Key:          key,
		ETag:         ptr.To(strings.ReplaceAll(uuid.NewString(), "-", "")),
		Size:         ptr.To(int64(len(data))),
		LastModified: &now,
	}

	_, ok := t.Buckets[bucket]
	if !ok {
		t.Buckets[bucket] = make(objval.TestBucket)
	}

	t.Buckets[bucket][key] = &objval.TestObject{
		ObjectAttrs: attrs,
		Body:        data,
	}

	return attrs.Key
}

func (t *TestClient) deleteKeysLocked(bucket, prefix string, include, exclude []*regexp.Regexp) {
	b := t.getBucketLocked(bucket)

	for key := range b {
		if strings.HasPrefix(key, prefix) && !ShouldIgnore(key, include, exclude) {
			delete(b, key)
		}
	}
}

// partKey returns a key which should be used for an in-progress multipart upload. This function should be used to
// generate key names since they'll be prefixed with 'basename(key)-mpu-' allowing efficient listing upon completion.
func partKey(id, key string) string {
	return path.Join(path.Dir(key), fmt.Sprintf("%s-mpu-%s-%s", path.Base(key), id, uuid.New()))
}

// partPrefix returns the prefix which will be used for all parts in the given upload for the provided key.
func partPrefix(id, key string) string {
	return fmt.Sprintf("%s-mpu-%s", key, id)
}

// parentDirectory returns the root directory for the provided key, or the key itself if it's the top-level.
func parentDirectory(key string) string {
	dir := path.Dir(key)
	if dir == "." || dir == "/" {
		return key
	}

	return dir
}
