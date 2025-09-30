// Package objutil provide utility functions for object store clients which expose more complex/configurable behavior
// than using a base 'objcli.Client'.
package objutil

import (
	"fmt"
	"io"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
	ioiface "github.com/couchbase/tools-common/types/v2/iface"
)

const (
	// MinPartSize is the minimum size allowed for 'PartSize', and is a hard limit enforced by AWS.
	MinPartSize = objaws.MinUploadSize

	// MPUThreshold is the threshold at which point we break the upload up into multiple requests which are executed
	// concurrently.
	//
	// NOTE: Multipart uploads generally require at least three requests, hence the choice of 'MinPartSize * 3'.
	MPUThreshold = MinPartSize * 3
)

// UploadOptions encapsulates the options available when using the 'Upload' function to upload data to a remote cloud.
type UploadOptions struct {
	Options

	// Client is the client used to perform the operation.
	//
	// NOTE: This attribute is required.
	Client objcli.Client

	// Bucket is the bucket to upload the object to.
	//
	// NOTE: This attribute is required.
	Bucket string

	// Key is the key for the object being uploaded.
	//
	// NOTE: This attribute is required.
	Key string

	// Body is the content which should be used for the body of the object.
	//
	// NOTE: This attribute is required.
	Body ioiface.ReadAtSeeker

	// MPUThreshold is a threshold at which point objects which broken down into multipart uploads.
	MPUThreshold int64

	// Precondition is used to perform a conditional operation. If the precondition is not satisfied the operation will
	// fail.
	Precondition objcli.OperationPrecondition

	// Lock is the object lock which determines the period during which the object will be immutable. If set to nil the
	// object will be mutable.
	//
	// NOTE: Verify that versioning/locking is enabled using `GetBucketLockingStatus` before setting a lock.
	Lock *objcli.ObjectLock
}

// defaults populates the options with sensible defaults.
func (u *UploadOptions) defaults() {
	u.Options.defaults()

	u.MPUThreshold = max(u.MPUThreshold, MPUThreshold)
}

// Upload an object to a remote cloud breaking it down into a multipart upload if the body is over a given size.
func Upload(opts UploadOptions) (*objval.ObjectAttrs, error) {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	length, err := objcli.SeekerLength(opts.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to determine length of body: %w", err)
	}

	// Under the threshold, upload using a single request
	if length > opts.MPUThreshold {
		return upload(opts)
	}

	attrs, err := opts.Client.PutObject(opts.Context, objcli.PutObjectOptions{
		Bucket:       opts.Bucket,
		Key:          opts.Key,
		Body:         opts.Body,
		Precondition: opts.Precondition,
		Lock:         opts.Lock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to put object: %w", err)
	}

	return attrs, nil
}

// upload an object to a remote cloud by breaking it down into individual chunks and uploading them concurrently.
func upload(opts UploadOptions) (*objval.ObjectAttrs, error) {
	mpu, err := NewMPUploader(MPUploaderOptions{
		Client:       opts.Client,
		Bucket:       opts.Bucket,
		Key:          opts.Key,
		Options:      opts.Options,
		Precondition: opts.Precondition,
		Lock:         opts.Lock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create uploader: %w", err)
	}
	defer mpu.Abort() //nolint:errcheck

	reader := NewChunkReader(opts.Body, opts.PartSize)

	err = reader.ForEach(func(chunk *io.SectionReader) error { return mpu.Upload(chunk) })
	if err != nil {
		return nil, fmt.Errorf("failed to queue chunks: %w", err)
	}

	attrs, err := mpu.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to complete upload: %w", err)
	}

	return attrs, nil
}
