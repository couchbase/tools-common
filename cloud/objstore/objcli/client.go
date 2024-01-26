// Package objcli exposes a unified 'Client' interface for accessing/managing objects stored in the cloud.
package objcli

import (
	"context"
	"io"
	"regexp"

	"github.com/couchbase/tools-common/cloud/v4/objstore/objval"
)

//go:generate mockery --name Client --case underscore --inpackage

// GetObjectOptions encapsulates the options available when using the 'GetObject' function.
type GetObjectOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Key is the key (path) of the object/blob being operated on.
	Key string

	// ByteRange allows specifying a start/end offset to be operated on.
	ByteRange *objval.ByteRange
}

// GetObjectAttrsOptions encapsulates the options available when using the 'GetObjectAttrs' function.
type GetObjectAttrsOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Key is the key (path) of the object/blob being operated on.
	Key string
}

// PutObjectOptions encapsulates the options available when using the 'PutObject' function.
type PutObjectOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Key is the key (path) of the object/blob being operated on.
	Key string

	// Body is the data that will be uploaded.
	//
	// NOTE: Required to be a 'ReadSeeker' to support checksum calculation/validation.
	Body io.ReadSeeker
}

// CopyObjectOptions encapsulates the options available when using the 'CopyObject' function.
type CopyObjectOptions struct {
	// DestinationBucket is the bucket the will be copied into.
	DestinationBucket string

	// DestinationKey is the key for the copied object.
	DestinationKey string

	// SourceBucket is the bucket containing the object being copied.
	SourceBucket string

	// SourceKey is the key of the object being copied.
	SourceKey string
}

// AppendToObjectOptions encapsulates the options available when using the 'AppendToObject' function.
type AppendToObjectOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Key is the key (path) of the object/blob being operated on.
	Key string

	// Body is the data that will be appended.
	Body io.ReadSeeker
}

// DeleteObjectsOptions encapsulates the options available when using the 'DeleteObjects' function.
type DeleteObjectsOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Keys are the keys that will be deleted.
	Keys []string
}

// DeleteDirectoryOptions encapsulates the options available when using the 'DeleteDirectory' function.
type DeleteDirectoryOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Prefix is the prefix that will be operated on.
	Prefix string
}

// IterateFunc is the function used when iterating over objects, this function will be called once for each object whose
// key matches the provided filtering.
type IterateFunc func(attrs *objval.ObjectAttrs) error

// IterateObjectsOptions encapsulates the options available when using the 'IterateObjects' function.
type IterateObjectsOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Prefix is the prefix that will listed.
	Prefix string

	// Delimiter use to group keys e.g. '/' causes listing to only occur within a "directory".
	Delimiter string

	// Include objects where the keys match any of the given regular expressions.
	Include []*regexp.Regexp

	// Exclude objects where the keys match any of the given regular expressions.
	Exclude []*regexp.Regexp

	// Func is executed for each object listed.
	Func IterateFunc
}

// CreateMultipartUploadOptions encapsulates the options available when using the 'CreateMultipartUpload' function.
type CreateMultipartUploadOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// Key is the key (path) of the object/blob being operated on.
	Key string
}

// ListPartsOptions encapsulates the options available when using the 'ListParts' function.
type ListPartsOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// UploadID - string
	UploadID string

	// Key is the key (path) of the object/blob being operated on.
	Key string
}

// UploadPartOptions encapsulates the options available when using the 'UploadPart' function.
type UploadPartOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// UploadID - string
	UploadID string

	// Key is the key (path) of the object/blob being operated on.
	Key string

	// Number is the number that will be assigned to the part.
	//
	// NOTE: Should be between 1-10,000 and is used for the ordering of parts upon completion.
	Number int

	// Body is the data that will be uploaded.
	Body io.ReadSeeker
}

// UploadPartCopyOptions encapsulates the options available when using the 'UploadPartCopy' function.
type UploadPartCopyOptions struct {
	// DestinationBucket is the bucket the will be copied into.
	DestinationBucket string

	// UploadID is the id of the upload being operated on.
	UploadID string

	// DestinationKey is the key for the copied object.
	DestinationKey string

	// SourceBucket is the bucket containing the object being copied.
	SourceBucket string

	// SourceKey is the key of the object being copied.
	SourceKey string

	// Number is the number that will be assigned to the part.
	//
	// NOTE: Should be between 1-10,000 and is used for the ordering of parts upon completion.
	Number int

	// ByteRange allows specifying a start/end offset to be operated on.
	//
	// NOTE: Not supported by all cloud providers.
	ByteRange *objval.ByteRange
}

// CompleteMultipartUploadOptions encapsulates the options available when using the 'CompleteMultipartUpload' function.
type CompleteMultipartUploadOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// UploadID is the id of the upload being operated on.
	UploadID string

	// Key is the key (path) of the object/blob being operated on.
	Key string

	// Parts is an ordered list of parts that should be constructed into the completed object.
	Parts []objval.Part
}

// AbortMultipartUploadOptions encapsulates the options available when using the 'AbortMultipartUpload' function.
type AbortMultipartUploadOptions struct {
	// Bucket is the bucket being operated on.
	Bucket string

	// UploadID is the id of the upload being operated on.
	UploadID string

	// Key is the key (path) of the object/blob being operated on.
	Key string
}

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
	GetObject(ctx context.Context, opts GetObjectOptions) (*objval.Object, error)

	// GetObjectAttrs returns general metadata about the object with the given key.
	GetObjectAttrs(ctx context.Context, opts GetObjectAttrsOptions) (*objval.ObjectAttrs, error)

	// PutObject creates an object in the cloud with the given key/options.
	PutObject(ctx context.Context, opts PutObjectOptions) error

	// CopyObject copies an object from one location to another, this may be within the same bucket.
	//
	// NOTE: Each cloud provider has limitations on the max size for copied objects therefore using this function
	// directly is not recommend; see 'objutil.CopyObject' which handles these nuances.
	CopyObject(ctx context.Context, opts CopyObjectOptions) error

	// AppendToObject appends the provided data to the object with the given key, this is a binary concatenation.
	//
	// NOTE: If the given object does not already exist, it will be created.
	AppendToObject(ctx context.Context, opts AppendToObjectOptions) error

	// DeleteObjects deletes all the objects with the given keys ignoring any errors for keys which are not found.
	//
	// NOTE: Depending on the underlying client and support from its SDK, this function may batch operations into pages.
	DeleteObjects(ctx context.Context, opts DeleteObjectsOptions) error

	// DeleteDirectory deletes all the objects which have the given prefix.
	//
	// NOTE: Depending on the underlying client and support from its SDK, this function may batch operations into pages.
	DeleteDirectory(ctx context.Context, opts DeleteDirectoryOptions) error

	// IterateObjects iterates through the objects a bucket running the provided iteration function for each object
	// which matches the given filtering parameters.
	IterateObjects(ctx context.Context, opts IterateObjectsOptions) error

	// CreateMultipartUpload creates a new multipart upload for the given key.
	//
	// NOTE: Not all clients directly support multipart uploads, the interface exposed should be used as if they do. The
	// underlying client will handle any nuances.
	CreateMultipartUpload(ctx context.Context, opts CreateMultipartUploadOptions) (string, error)

	// ListParts returns the list of parts staged or uploaded for the given upload id/key pair.
	//
	// NOTE: The returned parts will not have their part number populated as this is not stored by all cloud providers.
	ListParts(ctx context.Context, opts ListPartsOptions) ([]objval.Part, error)

	// UploadPart creates/uploads a new part for the multipart upload with the given id.
	UploadPart(ctx context.Context, opts UploadPartOptions) (objval.Part, error)

	// UploadPartCopy creates a new part for the multipart upload using an existing object (or part of an existing
	// object).
	UploadPartCopy(ctx context.Context, opts UploadPartCopyOptions) (objval.Part, error)

	// CompleteMultipartUpload completes the multipart upload with the given id, the given parts should be provided in
	// the order that they should be constructed.
	CompleteMultipartUpload(ctx context.Context, opts CompleteMultipartUploadOptions) error

	// AbortMultipartUpload aborts the multipart upload with the given id whilst cleaning up any abandoned parts.
	AbortMultipartUpload(ctx context.Context, opts AbortMultipartUploadOptions) error

	// Close the underlying client/SDK where applicable; use of the client, or the underlying SDK after a call to Close
	// has undefined behavior. This is required to stop memory leaks in GCP.
	Close() error
}
