package objval

import (
	"io"
	"time"
)

// ObjectAttrs represents the attributes usually attached to an object in the cloud.
type ObjectAttrs struct {
	// Object identity attributes
	Key  string
	ETag string // NOTE: Not populated during object iteration.

	// Attributes about the object itself
	Size         int64
	LastModified *time.Time
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
