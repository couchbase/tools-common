// Package objcli exposes a unified 'Client' interface for accessing/managing objects stored in the cloud.
package objcli

import (
	"context"
	"io"
	"regexp"

	"github.com/couchbase/tools-common/objstore/objval"
)

//go:generate mockery --name Client --case underscore --inpackage

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
	GetObject(ctx context.Context, bucket, key string, br *objval.ByteRange) (*objval.Object, error)

	// GetObjectAttrs returns general metadata about the object with the given key.
	GetObjectAttrs(ctx context.Context, bucket, key string) (*objval.ObjectAttrs, error)

	// PutObject creates an object in the cloud with the given key/options.
	//
	// NOTE: The body is required to be a 'ReadSeeker' to support checksum calculation/validation.
	PutObject(ctx context.Context, bucket, key string, body io.ReadSeeker) error

	// AppendToObject appends the provided data to the object with the given key, this is a binary concatenation.
	//
	// NOTE: If the given object does not already exist, it will be created.
	AppendToObject(ctx context.Context, bucket, key string, data io.ReadSeeker) error

	// DeleteObjects deletes all the objects with the given keys ignoring any errors for keys which are not found.
	//
	// NOTE: Depending on the underlying client and support from its SDK, this function may batch operations into pages.
	DeleteObjects(ctx context.Context, bucket string, keys ...string) error

	// DeleteDirectory deletes all the objects which have the given prefix.
	//
	// NOTE: Depending on the underlying client and support from its SDK, this function may batch operations into pages.
	DeleteDirectory(ctx context.Context, bucket, prefix string) error

	// IterateObjects iterates through the objects a bucket running the provided iteration function for each object
	// which matches the given filtering parameters.
	IterateObjects(
		ctx context.Context, bucket, prefix, delimiter string, include, exclude []*regexp.Regexp, fn IterateFunc,
	) error

	// CreateMultipartUpload creates a new multipart upload for the given key.
	//
	// NOTE: Not all clients directly support multipart uploads, the interface exposed should be used as if they do. The
	// underlying client will handle any nuances.
	CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error)

	// ListParts returns the list of parts staged or uploaded for the given upload id/key pair.
	//
	// NOTE: The returned parts will not have their part number populated as this is not stored by all cloud providers.
	ListParts(ctx context.Context, bucket, id, key string) ([]objval.Part, error)

	// UploadPart creates/uploads a new part for the multipart upload with the given id.
	//
	// NOTE: The part 'number' should be between 1-10,000 and is used for the ordering of parts upon completion.
	UploadPart(ctx context.Context, bucket, id, key string, number int, body io.ReadSeeker) (objval.Part, error)

	// UploadPartCopy creates a new part for the multipart upload using an existing object (or part of an existing
	// object).
	//
	// NOTE: Not all cloud providers support providing a byte range.
	UploadPartCopy(
		ctx context.Context, bucket, id, dst, src string, number int, br *objval.ByteRange,
	) (objval.Part, error)

	// CompleteMultipartUpload completes the multipart upload with the given id, the given parts should be provided in
	// the order that they should be constructed.
	CompleteMultipartUpload(ctx context.Context, bucket, id, key string, parts ...objval.Part) error

	// AbortMultipartUpload aborts the multipart upload with the given id whilst cleaning up any abandoned parts.
	AbortMultipartUpload(ctx context.Context, bucket, id, key string) error
}
