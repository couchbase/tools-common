package objcli

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/tools-common/objstore/objerr"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/testutil"
	"github.com/google/uuid"
)

// TestClient implementation of the 'Client' interface which stores state in memory, and can be used to avoid having to
// manually mock a client during unit testing.
type TestClient struct {
	t    *testing.T
	lock sync.Mutex

	// Buckets is the in memory state maintained by the client. Internally, access is guarded by a mutex, however, it's
	// not safe/recommended to access this attribute whilst a test is running; it should only be used to inspect state
	// (to perform assertions) once testing is complete.
	Buckets objval.TestBuckets
}

var _ Client = (*TestClient)(nil)

// NewTestClient returns a new test client, which has no buckets/objects.
func NewTestClient(t *testing.T) *TestClient {
	return &TestClient{
		t:       t,
		Buckets: make(objval.TestBuckets),
	}
}

func (t *TestClient) GetObject(bucket, key string, br *objval.ByteRange) (*objval.Object, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, err := t.getObjectLocked(bucket, key)
	if err != nil {
		return nil, err
	}

	return &objval.Object{
		ObjectAttrs: object.ObjectAttrs,
		Body:        io.NopCloser(bytes.NewReader(object.Body)),
	}, nil
}

func (t *TestClient) GetObjectAttrs(bucket, key string) (*objval.ObjectAttrs, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	object, err := t.getObjectLocked(bucket, key)
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
		t.putObjectLocked(bucket, key, data)
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

func (t *TestClient) IterateObjects(bucket, prefix string, include, exclude []*regexp.Regexp, fn IterateFunc) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	b := t.getBucketLocked(bucket)

	for key, object := range b {
		if !strings.HasPrefix(key, prefix) || ShouldIgnore(key, include, exclude) {
			continue
		}

		if err := fn(&object.ObjectAttrs); err != nil {
			return err
		}
	}

	return nil
}

func (t *TestClient) CreateMultipartUpload(bucket, key string) (string, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	return uuid.NewString(), nil
}

func (t *TestClient) UploadPart(bucket, id, key string, number int, body io.ReadSeeker) (objval.Part, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	part := objval.Part{
		ID:     t.putObjectLocked(bucket, fmt.Sprintf("%s-%s-%d", id, key, number), body),
		Number: number,
	}

	return part, nil
}

func (t *TestClient) CompleteMultipartUpload(bucket, id, key string, parts ...objval.Part) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	buffer := &bytes.Buffer{}

	for _, part := range parts {
		object, err := t.getObjectLocked(bucket, fmt.Sprintf("%s-%s-%d", id, key, part.Number))
		if err != nil {
			return err
		}

		buffer.Write(object.Body)
	}

	_ = t.putObjectLocked(bucket, key, bytes.NewReader(buffer.Bytes()))

	return t.abortMultipartUploadLocked(bucket, id, key, parts...)
}

func (t *TestClient) AbortMultipartUpload(bucket, id, key string, parts ...objval.Part) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.abortMultipartUploadLocked(bucket, id, key, parts...)
}

func (t *TestClient) getBucketLocked(bucket string) objval.TestBucket {
	_, ok := t.Buckets[bucket]
	if !ok {
		t.Buckets[bucket] = make(objval.TestBucket)
	}

	return t.Buckets[bucket]
}

func (t *TestClient) getObjectLocked(bucket, key string) (*objval.TestObject, error) {
	o, ok := t.getBucketLocked(bucket)[key]
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

	return attrs.ETag
}

func (t *TestClient) abortMultipartUploadLocked(bucket, id, key string, parts ...objval.Part) error {
	b := t.getBucketLocked(bucket)

	for _, part := range parts {
		delete(b, fmt.Sprintf("%s-%s-%d", id, key, part.Number))
	}

	return nil
}
