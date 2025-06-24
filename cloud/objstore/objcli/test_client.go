package objcli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objerr"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	testutil "github.com/couchbase/tools-common/testing/util"
	"github.com/couchbase/tools-common/types/v2/ptr"
	"github.com/couchbase/tools-common/types/v2/timeprovider"
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

	TimeProvider timeprovider.TimeProvider
}

var _ Client = (*TestClient)(nil)

// NewTestClient returns a new test client, which has no buckets/objects.
func NewTestClient(t *testing.T, provider objval.Provider) *TestClient {
	return &TestClient{
		t:            t,
		provider:     provider,
		Buckets:      make(objval.TestBuckets),
		TimeProvider: timeprovider.CurrentTimeProvider{},
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

	if opts.Precondition == OperationPreconditionOnlyIfAbsent {
		b := t.getBucketLocked(opts.Bucket)
		for objID := range b {
			if objID.Key == opts.Key {
				return errors.New("object already exists")
			}
		}
	}

	_, err := t.putObjectLocked(opts)
	if err != nil {
		return err
	}

	return nil
}

func (t *TestClient) CopyObject(_ context.Context, opts CopyObjectOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	src, err := t.getObjectRLocked(opts.SourceBucket, opts.SourceKey)
	if err != nil {
		return err
	}

	_, err = t.putObjectLocked(PutObjectOptions{
		Bucket: opts.DestinationBucket,
		Key:    opts.DestinationKey,
		Body:   bytes.NewReader(src.Body),
	})
	if err != nil {
		return err
	}

	return nil
}

func (t *TestClient) AppendToObject(_ context.Context, opts AppendToObjectOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, ok := t.getBucketLocked(opts.Bucket)[objval.TestObjectIdentifier{Key: opts.Key}]
	if ok {
		object.Body = append(object.Body, testutil.ReadAll(t.t, opts.Body)...)
	} else {
		_, err := t.putObjectLocked(PutObjectOptions{
			Bucket: opts.Bucket,
			Key:    opts.Key,
			Body:   opts.Body,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TestClient) DeleteObjects(_ context.Context, opts DeleteObjectsOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(opts.Bucket)

	for _, key := range opts.Keys {
		obj, err := t.getObjectRLocked(opts.Bucket, key)
		if err != nil {
			continue
		}

		if obj.LockExpiration != nil && obj.LockExpiration.After(t.TimeProvider.Now()) {
			return errors.New("cannot delete locked object")
		}

		delete(b, objval.TestObjectIdentifier{Key: key})
	}

	return nil
}

func (t *TestClient) DeleteObjectVersions(_ context.Context, opts DeleteObjectVersionsOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(opts.Bucket)

	for _, objVersion := range opts.Versions {
		obj, err := t.getObjectVersion(opts.Bucket, objVersion.Key, objVersion.VersionID)
		if err != nil {
			return err
		}

		if obj.LockExpiration != nil && obj.LockExpiration.After(t.TimeProvider.Now()) {
			return errors.New("cannot delete locked object")
		}

		delete(b, objval.TestObjectIdentifier(objVersion))
	}

	return nil
}

func (t *TestClient) DeleteDirectory(_ context.Context, opts DeleteDirectoryOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(opts.Bucket)

	for objID := range b {
		if !opts.Versions && objID.VersionID != "" {
			continue
		}

		if !strings.HasPrefix(objID.Key, opts.Prefix) {
			continue
		}

		obj, err := t.getObjectVersion(opts.Bucket, objID.Key, objID.VersionID)
		if err != nil {
			return err
		}

		if obj.LockExpiration != nil && obj.LockExpiration.After(t.TimeProvider.Now()) {
			return errors.New("cannot delete locked object")
		}

		delete(b, objID)
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

	for objID, object := range cpy {
		if !opts.Versions && objID.VersionID != "" {
			continue
		}

		if !strings.HasPrefix(objID.Key, opts.Prefix) || ShouldIgnore(objID.Key, opts.Include, opts.Exclude) {
			continue
		}

		var (
			attrs   = object.ObjectAttrs
			trimmed = strings.TrimPrefix(objID.Key, opts.Prefix)
		)

		// If this is a nested key, convert it into a directory stub. AWS allows a filesystem style API when you pass a
		// delimiter - if your prefix has a "directory" in it we get a stub, rather than the actual object which could
		// be nested.
		if opts.Delimiter != "" && strings.Count(trimmed, opts.Delimiter) > 1 {
			attrs.Key = parentDirectory(objID.Key)
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

	id, err := t.putObjectLocked(PutObjectOptions{
		Bucket:       opts.Bucket,
		Key:          partKey(opts.UploadID, opts.Key),
		Body:         opts.Body,
		Precondition: opts.Precondition,
		Lock:         opts.Lock,
	})
	if err != nil {
		return objval.Part{}, err
	}

	part := objval.Part{
		ID:     id,
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

	id, err := t.putObjectLocked(PutObjectOptions{
		Bucket: opts.DestinationBucket,
		Key:    partKey(opts.UploadID, opts.DestinationKey),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return objval.Part{}, err
	}

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

	_, err := t.putObjectLocked(PutObjectOptions{
		Bucket:       opts.Bucket,
		Key:          opts.Key,
		Body:         bytes.NewReader(buffer.Bytes()),
		Precondition: opts.Precondition,
		Lock:         opts.Lock,
	})
	if err != nil {
		return err
	}

	_ = t.deleteKeysLocked(opts.Bucket, partPrefix(opts.UploadID, opts.Key), nil, nil)

	return nil
}

func (t *TestClient) AbortMultipartUpload(_ context.Context, opts AbortMultipartUploadOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	err := t.deleteKeysLocked(opts.Bucket, partPrefix(opts.UploadID, opts.Key), nil, nil)
	if err != nil {
		return err
	}

	return nil
}

func (t *TestClient) GetBucketLockingStatus(
	_ context.Context,
	_ GetBucketLockingStatusOptions,
) (*objval.BucketLockingStatus, error) {
	return &objval.BucketLockingStatus{Enabled: true}, nil
}

func (t *TestClient) SetObjectLock(_ context.Context, opts SetObjectLockOptions) error {
	b, ok := t.Buckets[opts.Bucket]
	if !ok {
		return &objerr.NotFoundError{Type: "object", Name: opts.Key}
	}

	o, ok := b[objval.TestObjectIdentifier{Key: opts.Key, VersionID: opts.VersionID}]
	if !ok {
		return &objerr.NotFoundError{Type: "object", Name: opts.Key}
	}

	o.LockType = opts.Lock.Type
	o.LockExpiration = &opts.Lock.Expiration

	return nil
}

func (t *TestClient) getBucketLocked(bucket string) objval.TestBucket {
	_, ok := t.Buckets[bucket]
	if !ok {
		t.Buckets[bucket] = make(objval.TestBucket)
	}

	return t.Buckets[bucket]
}

func (t *TestClient) getObjectVersion(bucket, key, versionID string) (*objval.TestObject, error) {
	b, ok := t.Buckets[bucket]
	if !ok {
		return nil, &objerr.NotFoundError{Type: "object", Name: key}
	}

	o, ok := b[objval.TestObjectIdentifier{Key: key, VersionID: versionID}]
	if !ok {
		return nil, &objerr.NotFoundError{Type: "object", Name: key}
	}

	return o, nil
}

// NOTE: Buckets are automatically created by the test client when they're required, so this error returns an object not
// found error if either the bucket/object don't exist.
func (t *TestClient) getObjectRLocked(bucket, key string) (*objval.TestObject, error) {
	b, ok := t.Buckets[bucket]
	if !ok {
		return nil, &objerr.NotFoundError{Type: "object", Name: key}
	}

	o, ok := b[objval.TestObjectIdentifier{Key: key}]
	if !ok {
		return nil, &objerr.NotFoundError{Type: "object", Name: key}
	}

	return o, nil
}

func (t *TestClient) putObjectLocked(opts PutObjectOptions) (string, error) {
	body := opts.Body
	key := opts.Key
	bucket := opts.Bucket

	var (
		now  = t.TimeProvider.Now()
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

	oldVersion, ok := t.Buckets[bucket][objval.TestObjectIdentifier{Key: key}]
	if ok {
		if opts.Precondition == OperationPreconditionOnlyIfAbsent {
			return "", errors.New("object already exists")
		}

		versionID, _ := uuid.NewRandom()
		t.Buckets[bucket][objval.TestObjectIdentifier{Key: key, VersionID: versionID.String()}] = oldVersion
	}

	obj := &objval.TestObject{
		ObjectAttrs: attrs,
		Body:        data,
	}

	if opts.Lock != nil {
		switch opts.Lock.Type {
		case objval.LockTypeCompliance:
			obj.LockType = objval.LockTypeCompliance
			obj.LockExpiration = &opts.Lock.Expiration
		default:
			return "", errors.New("unported lock type")
		}
	}

	t.Buckets[bucket][objval.TestObjectIdentifier{Key: key}] = obj

	return attrs.Key, nil
}

func (t *TestClient) deleteKeysLocked(bucket, prefix string, include, exclude []*regexp.Regexp) error {
	b := t.getBucketLocked(bucket)

	for objID := range b {
		if strings.HasPrefix(objID.Key, prefix) && !ShouldIgnore(objID.Key, include, exclude) {
			obj, err := t.getObjectRLocked(bucket, objID.Key)
			if err != nil {
				return err
			}

			if obj.LockExpiration != nil && obj.LockExpiration.After(t.TimeProvider.Now()) {
				return errors.New("cannot delete locked object")
			}

			delete(b, objID)
		}
	}

	return nil
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
