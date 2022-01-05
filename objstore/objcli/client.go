// Package objcli exposes a unified 'Client' interface for accessing/managing objects stored in the cloud.
package objcli

import (
	"io"
	"regexp"

	"github.com/couchbase/tools-common/objstore/objval"
)

// IterateFunc is the function used when iterating over objects, this function will be called once for each object whose
// key matches the provided filtering.
type IterateFunc func(attrs *objval.ObjectAttrs) error

// Client is a unified interface for accessing/managing objects stored in the cloud.
type Client interface {
	// Provider returns the cloud provider this client is interfacing with.
	//
	// NOTE: This may be used to change high level behavior which may be cloud provider specific.
	Provider() objval.Provider

	// GetObject retrieves an object form the cloud, an optional byte range argument may be supplied which causes only
	// the requested byte range to be returned.
	//
	// NOTE: The returned objects body must be closed to avoid resource leaks.
	GetObject(bucket, key string, br *objval.ByteRange) (*objval.Object, error)

	// GetObjectAttrs returns general metadata about the object with the given key.
	GetObjectAttrs(bucket, key string) (*objval.ObjectAttrs, error)

	// PutObject creates an object in the cloud with the given key/options.
	//
	// NOTE: The body is required to be a 'ReadSeeker' to support checksum calculation/validation.
	PutObject(bucket, key string, body io.ReadSeeker) error

	// AppendToObject appends the provided data to the object with the given key, this is a binary concatenation.
	//
	// NOTE: If the given object does not already exist, it will be created.
	AppendToObject(bucket, key string, data io.ReadSeeker) error

	// DeleteObjects deletes all the objects with the given keys ignoring any errors for keys which are not found.
	//
	// NOTE: Depending on the underlying client and support from its SDK, this function may batch operations into pages.
	DeleteObjects(bucket string, keys ...string) error

	// DeleteDirectory deletes all the objects which have the given prefix.
	//
	// NOTE: Depending on the underlying client and support from its SDK, this function may batch operations into pages.
	DeleteDirectory(bucket, prefix string) error

	// IterateObjects iterates through the objects a bucket running the provided iteration function for each object
	// which matches the given filtering parameters.
	IterateObjects(bucket, prefix string, include, exclude []*regexp.Regexp, fn IterateFunc) error

	// CreateMultipartUpload creates a new multipart upload for the given key.
	//
	// NOTE: Not all clients directly support multipart uploads, the interface exposed should be used as if they do. The
	// underlying client will handle any nuances.
	CreateMultipartUpload(bucket, key string) (string, error)

	// UploadPart creates/uploads a new part for the multipart upload with the given id.
	//
	// NOTE: The part 'number' should be between 1-10,000 and is used for the ordering of parts upon completion.
	UploadPart(bucket, id, key string, number int, body io.ReadSeeker) (objval.Part, error)

	// CompleteMultipartUpload completes the multipart upload with the given id, the given parts should be provided in
	// the order that they should be constructed.
	CompleteMultipartUpload(bucket, id, key string, parts ...objval.Part) error

	// AbortMultipartUpload aborts the multipart upload with the given id whilst cleaning up any abandoned parts.
	//
	// NOTE: Providing the list of completed parts is not necessary for all cloud providers, however, is recommend.
	AbortMultipartUpload(bucket, id, key string, parts ...objval.Part) error
}
