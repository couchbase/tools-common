package objval

import (
	"io"
	"time"
)

// ObjectVersion represents a version of an object.
type ObjectVersion struct {
	// Key is the identifier for the object; a unique path.
	Key string
	// VersionID is used to identify a specific version when object versioning is enabled.
	VersionID string
}

// ObjectAttrs represents the attributes usually attached to an object in the cloud.
type ObjectAttrs struct {
	// Key is the identifier for the object; a unique path.
	Key string

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

	// VersionID is used to identify a specific version when object versioning is enabled.
	VersionID string

	// LockExpiration is the time the object lock will expire.
	LockExpiration *time.Time

	// LockType is the type of the object lock.
	LockType LockType

	// IsCurrentVersion is used to determine whether this is the latest
	// version of the object. Only available when iterating object versions on AWS and Azure.
	IsCurrentVersion bool

	// IsDeleteMarker determines whether this describes a delete marker instead of an object or a version.
	IsDeleteMarker bool
}

// IsDir returns a boolean indicating whether these attributes represent a synthetic directory, created by the library
// when iterating objects using a 'delimiter'. When 'IsDir' returns 'true', only the 'Key' attribute will be populated.
//
// NOTE: This does not, and will not indicate whether the remote object is itself a directory stub; a zero length object
// created by the AWS WebUI.
func (o *ObjectAttrs) IsDir() bool {
	return o.Size == nil && o.ETag == nil && o.LastModified == nil && !o.IsDeleteMarker
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
type TestBucket map[TestObjectIdentifier]*TestObject

// TestObjectIdentifier identifies an object or an object version and is only used by the 'TestObject'.
type TestObjectIdentifier struct {
	// Key is the object key.
	Key string
	// VersionID identifies the specific object version. If empty the identifier refers to the current version.
	VersionID string
}

// TestObject represents an object and is only used by the 'TestObject'.
type TestObject struct {
	ObjectAttrs
	// Body is data contained in the test object.
	Body []byte
}
