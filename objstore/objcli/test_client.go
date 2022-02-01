package objcli

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/testutil"
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

func (t *TestClient) GetObject(bucket, key string, br *objval.ByteRange) (*objval.Object, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	object, err := t.getObjectRLocked(bucket, key)
	if err != nil {
		return nil, err
	}

	var offset, length int64 = 0, int64(len(object.Body) + 1)
	if br != nil {
		offset, length = br.ToOffsetLength(length)
	}

	return &objval.Object{
		ObjectAttrs: object.ObjectAttrs,
		Body:        io.NopCloser(io.NewSectionReader(bytes.NewReader(object.Body), offset, length)),
	}, nil
}

func (t *TestClient) GetObjectAttrs(bucket, key string) (*objval.ObjectAttrs, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	object, err := t.getObjectRLocked(bucket, key)
	if err != nil {
		return nil, err
	}

	return &object.ObjectAttrs, nil
}

func (t *TestClient) PutObject(bucket, key string, body io.ReadSeeker) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_ = t.putObjectLocked(bucket, key, body)

	return nil
}

func (t *TestClient) AppendToObject(bucket, key string, data io.ReadSeeker) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, ok := t.getBucketLocked(bucket)[key]
	if ok {
		object.Body = append(object.Body, testutil.ReadAll(t.t, data)...)
	} else {
		_ = t.putObjectLocked(bucket, key, data)
	}

	return nil
}

func (t *TestClient) DeleteObjects(bucket string, keys ...string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(bucket)

	for _, key := range keys {
		delete(b, key)
	}

	return nil
}

func (t *TestClient) DeleteDirectory(bucket, prefix string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(bucket)

	for key := range b {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		delete(b, key)
	}

	return nil
}

func (t *TestClient) IterateObjects(bucket, prefix, delimiter string, include, exclude []*regexp.Regexp,
	fn IterateFunc) error {
	if include != nil && exclude != nil {
		return ErrIncludeAndExcludeAreMutuallyExclusive
	}

	t.lock.RLock()
	defer t.lock.RUnlock()

	b, ok := t.Buckets[bucket]
	if !ok {
		return nil
	}

	for key, object := range b {
		if !strings.HasPrefix(key, prefix) || ShouldIgnore(key, include, exclude) {
			continue
		}

		var (
			trimmed = strings.TrimPrefix(key, prefix)
			attrs   = object.ObjectAttrs
		)

		// If this is a nested key, convert it into a directory stub
		if delimiter != "" && strings.Count(trimmed, delimiter) > 1 {
			attrs.Key = rootDirectory(trimmed)
			attrs.Size = 0
			attrs.LastModified = nil
		}

		if err := fn(&attrs); err != nil {
			return err
		}
	}

	return nil
}

func (t *TestClient) CreateMultipartUpload(bucket, key string) (string, error) {
	return uuid.NewString(), nil
}

func (t *TestClient) ListParts(bucket, id, key string) ([]objval.Part, error) {
	var (
		prefix = partPrefix(id, key)
		parts  = make([]objval.Part, 0)
	)

	fn := func(attrs *objval.ObjectAttrs) error {
		if strings.HasPrefix(attrs.Key, prefix) {
			parts = append(parts, objval.Part{ID: attrs.Key, Size: attrs.Size})
		}

		return nil
	}

	err := t.IterateObjects(bucket, prefix, "/", nil, nil, fn)
	if err != nil {
		return nil, fmt.Errorf("failed to iterate objects: %w", err)
	}

	return parts, nil
}

func (t *TestClient) UploadPart(bucket, id, key string, number int, body io.ReadSeeker) (objval.Part, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	size, err := aws.SeekerLen(body)
	require.NoError(t.t, err)

	part := objval.Part{
		ID:     t.putObjectLocked(bucket, partKey(id, key), body),
		Number: number,
		Size:   size,
	}

	return part, nil
}

func (t *TestClient) UploadPartCopy(bucket, id, dst, src string, number int,
	br *objval.ByteRange) (objval.Part, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, err := t.getObjectRLocked(bucket, src)
	if err != nil {
		return objval.Part{}, err
	}

	body := make([]byte, br.End-br.Start+1)
	copy(body, object.Body)

	part := objval.Part{
		ID:     t.putObjectLocked(bucket, partKey(id, dst), bytes.NewReader(body)),
		Number: number,
		Size:   int64(len(body)),
	}

	return part, nil
}

func (t *TestClient) CompleteMultipartUpload(bucket, id, key string, parts ...objval.Part) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	buffer := &bytes.Buffer{}

	for _, part := range parts {
		object, err := t.getObjectRLocked(bucket, part.ID)
		if err != nil {
			return err
		}

		buffer.Write(object.Body)
	}

	_ = t.putObjectLocked(bucket, key, bytes.NewReader(buffer.Bytes()))

	return t.deleteKeysLocked(bucket, partPrefix(id, key), nil, nil)
}

func (t *TestClient) AbortMultipartUpload(bucket, id, key string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.deleteKeysLocked(bucket, partPrefix(id, key), nil, nil)
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
		ETag:         strings.ReplaceAll(uuid.NewString(), "-", ""),
		Size:         int64(len(data)),
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

func (t *TestClient) deleteKeysLocked(bucket, prefix string, include, exclude []*regexp.Regexp) error {
	b := t.getBucketLocked(bucket)

	for key := range b {
		if strings.HasPrefix(key, prefix) && !ShouldIgnore(key, include, exclude) {
			delete(b, key)
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

// rootDirectory returns the root directory for the provided key.
func rootDirectory(key string) string {
	dir := path.Dir(key)
	if dir == "." || dir == "/" {
		return key
	}

	return rootDirectory(dir)
}
