// Package objutil provide utility functions for object store clients which expose more complex/configurable behavior
// than using a base 'objcli.Client'.
package objutil

import (
	"context"
	"fmt"
	"io"

	"github.com/couchbase/tools-common/maths"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objcli/objaws"

	"github.com/aws/aws-sdk-go/aws"
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

// ReadAtSeeker is a composition of the reader/seeker/reader at interfaces.
type ReadAtSeeker interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

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
	Body ReadAtSeeker

	// MPUThreshold is a threshold at which point objects which broken down into multipart uploads.
	MPUThreshold int64
}

// defaults populates the options with sensible defaults.
func (u *UploadOptions) defaults() {
	u.PartSize = maths.Max(u.PartSize, MinPartSize)
	u.MPUThreshold = maths.Max(u.MPUThreshold, MPUThreshold)

	if u.Options.Context == nil {
		u.Options.Context = context.Background()
	}
}

// Upload an object to a remote cloud breaking it down into a multipart upload if the body is over a given size.
func Upload(opts UploadOptions) error {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	length, err := aws.SeekerLen(opts.Body)
	if err != nil {
		return fmt.Errorf("failed to determine length of body: %w", err)
	}

	// Under the threshold, upload using a single request
	if length <= opts.MPUThreshold {
		return opts.Client.PutObject(opts.Context, opts.Bucket, opts.Key, opts.Body)
	}

	return upload(opts)
}

// upload an object to a remote cloud by breaking it down into individual chunks and uploading them concurrently.
func upload(opts UploadOptions) error {
	mpu, err := NewMPUploader(MPUploaderOptions{
		Client:  opts.Client,
		Bucket:  opts.Bucket,
		Key:     opts.Key,
		Options: opts.Options,
	})
	if err != nil {
		return fmt.Errorf("failed to create uploader: %w", err)
	}
	defer mpu.Abort() //nolint:errcheck,wsl

	reader := NewChunkReader(opts.Body, opts.PartSize)

	err = reader.ForEach(func(chunk *io.SectionReader) error { return mpu.Upload(chunk) })
	if err != nil {
		return fmt.Errorf("failed to queue chunks: %w", err)
	}

	err = mpu.Commit()
	if err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}

	return nil
}
