package objval

import (
	"io"
	"time"
)

// ObjectAttrs represents the attributes usually attached to an object in the cloud.
type ObjectAttrs struct {
	// Key is the identifier for the object; a unique path.
	Key string

	// Version is the unique version of the object. This is optional.
	Version any

	// ETag is the HTTP entity tag for the object, each cloud provider uses this differently with different rules also
	// applying to different scenarios (e.g. multipart uploads).
	//
	// NOTE: Not populated during object iteration.
	ETag *string

	// Size is the size or content length of the object in bytes.
	//
	// NOTE: May be conditionally populated by 'GetObject', for example when a chunked response is returned, this
	// attribute will be <nil>.
	Size *int64

	// LastModified is the time the object was last updated (or created).
	//
	// NOTE: The semantics of this attribute may differ between cloud providers (e.g. an change of metadata might bump
	// the last modified time).
	LastModified *time.Time
}

// IsDir returns a boolean indicating whether these attributes represent a synthetic directory, created by the library
// when iterating objects using a 'delimiter'. When 'IsDir' returns 'true', only the 'Key' attribute will be populated.
//
// NOTE: This does not, and will not indicate whether the remote object is itself a directory stub; a zero length object
// created by the AWS WebUI.
func (o *ObjectAttrs) IsDir() bool {
	return o.Size == nil && o.ETag == nil && o.LastModified == nil
}

// Object represents an object stored in the cloud, simply the attributes and it's body.
type Object struct {
	ObjectAttrs

	// This body will generally be a HTTP response body; it should be read once, and closed to avoid resource leaks.
	//
	// NOTE: Depending on the request type, this may not be the entire body of the object, just a byte range.
	Body io.ReadCloser
}

// TestBuckets represents a number of buckets, and is only used by the 'TestClient' to store state in memory.
type TestBuckets map[string]TestBucket

// TestBucket represents a bucket and is only used by the 'TestClient' to store objects in memory.
type TestBucket map[string]*TestObject

// TestObject represents an object and is only used by the 'TestObject'.
type TestObject struct {
	ObjectAttrs
	Body []byte
}
