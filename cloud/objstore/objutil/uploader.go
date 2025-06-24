package objutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	"github.com/couchbase/tools-common/sync/v2/hofp"
)

// MaxUploadParts is the hard limit on the number of parts that can be uploaded by a 'MPUploader'.
const MaxUploadParts = objaws.MaxUploadParts

var (
	// ErrMPUploaderExceededMaxPartCount is returned if the user attempts to upload more than 'MaxUploadParts' parts.
	ErrMPUploaderExceededMaxPartCount = errors.New("exceeded maximum number of upload parts")

	// ErrMPUploaderAlreadyStopped is returned if the upload is stopped multiple times.
	ErrMPUploaderAlreadyStopped = errors.New("upload has already been stopped")
)

// OnPartCompleteFunc is a readability wrapper around a callback function which may be run after each part has been
// uploaded.
type OnPartCompleteFunc func(metadata any, part objval.Part) error

// MPUploaderOptions encapsulates the options available when creating a 'MPUploader'.
type MPUploaderOptions struct {
	Options

	// Client is the client used to perform the operation.
	//
	// NOTE: This attribute is required.
	Client objcli.Client

	// Bucket is the bucket to upload the object to.
	//
	// NOTE: This attribute is required.
	Bucket string

	// ID is the id for an in-progress multipart upload that should be "continued".
	//
	// NOTE: Here be dragons, no validation takes place to ensure an upload with the provided id exists.
	ID string

	// Key is the key for the object being uploaded.
	//
	// NOTE: This attribute is required.
	Key string

	// Parts is the list of parts for an in-progress multipart upload which is being continued. Should be supplied in
	// conjunction with 'ID'.
	//
	// NOTE: Here be dragons, no validation takes place to ensure these parts are still available.
	Parts []objval.Part

	// OnPartComplete is a callback which is run after successfully uploading each part.
	//
	// This function:
	// 1. Should not block, since it will block other parts from uploading
	// 2. Will not be called concurrently by multiple goroutines
	// 3. Will be called "out-of-order", parts may be completed in any arbitrary order
	//
	// This callback may be used to track parts and persist them to disk to allow robust multipart uploads.
	OnPartComplete OnPartCompleteFunc

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
func (m *MPUploaderOptions) defaults() {
	m.Options.defaults()

	if m.OnPartComplete == nil {
		m.OnPartComplete = func(_ any, _ objval.Part) error { return nil }
	}
}

// MPUploader is a multipart uploader which adds parts to a remote multipart upload whilst concurrently uploading data
// using a worker pool.
type MPUploader struct {
	opts    MPUploaderOptions
	number  int
	pool    *hofp.Pool
	lock    sync.Mutex
	stopped uint32
}

// NewMPUploader creates a new multipart uploader using the given options, this will create a new multipart upload if
// one hasn't already been provided.
//
// NOTE: Either Commit or Abort should be called to avoid resource leaks.
func NewMPUploader(opts MPUploaderOptions) (*MPUploader, error) {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	uploader := &MPUploader{
		opts: opts,
	}

	// Continue from where the last part was uploaded (if provided)
	for _, part := range uploader.opts.Parts {
		uploader.number = max(uploader.number, part.Number)
	}

	err := uploader.createMPU()
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	// Only create the pool after successfully creating the multipart upload to avoid having to handle cleanup
	uploader.pool = hofp.NewPool(hofp.Options{})

	return uploader, nil
}

// create a new multipart upload if one is not already in-progress.
func (m *MPUploader) createMPU() error {
	if m.opts.ID != "" {
		return nil
	}

	var err error

	m.opts.ID, err = m.opts.Client.CreateMultipartUpload(m.opts.Context, objcli.CreateMultipartUploadOptions{
		Bucket: m.opts.Bucket,
		Key:    m.opts.Key,
		Lock:   m.opts.Lock,
	})

	return err
}

// UploadID returns the upload id created by the multipart uploader.
//
// NOTE: Depending on the underlying client, this upload id may be empty.
func (m *MPUploader) UploadID() string {
	return m.opts.ID
}

// Upload the given body as a part for the multipart upload.
//
// NOTE: This function is not thread safe.
func (m *MPUploader) Upload(body io.ReadSeeker) error {
	return m.UploadWithMeta(nil, body)
}

// UploadWithMeta uploads the given body as a part for the multipart upload. The provided metadata will be returned
// unmodified via the 'OnPartComplete' callback and may be used to pass metadata that may be persisted to disk at the
// same time as the completed part.
//
// NOTE: This function is not thread safe.
func (m *MPUploader) UploadWithMeta(metadata any, body io.ReadSeeker) error {
	if len(m.opts.Parts) >= MaxUploadParts {
		return ErrMPUploaderExceededMaxPartCount
	}

	m.number++

	queue := func(number int, body io.ReadSeeker) error {
		return m.pool.Queue(func(ctx context.Context) error { return m.upload(ctx, number, metadata, body) })
	}

	return queue(m.number, body)
}

// upload a new part with the given number/body.
func (m *MPUploader) upload(ctx context.Context, number int, metadata any, body io.ReadSeeker) error {
	part, err := m.opts.Client.UploadPart(ctx, objcli.UploadPartOptions{
		Bucket:       m.opts.Bucket,
		UploadID:     m.opts.ID,
		Key:          m.opts.Key,
		Number:       number,
		Body:         body,
		Precondition: m.opts.Precondition,
		Lock:         m.opts.Lock,
	})
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	// Parts may be uploaded concurrently, but must be marked as completed one at a time
	m.lock.Lock()
	defer m.lock.Unlock()

	// Insert prior to running 'OnPartComplete' to ensure 'Abort' correctly cleans up this part in the event that the
	// users callback returns an error.
	m.opts.Parts = append(m.opts.Parts, part)

	err = m.opts.OnPartComplete(metadata, part)
	if err != nil {
		return fmt.Errorf("failed to run 'OnPartComplete' callback: %w", err)
	}

	return nil
}

// Stop the worker pool without committing/aborting the upload.
//
// NOTE: Using the uploader after calling 'Stop' will lead to undefined behavior.
func (m *MPUploader) Stop() error {
	if !atomic.CompareAndSwapUint32(&m.stopped, 0, 1) {
		return ErrMPUploaderAlreadyStopped
	}

	return m.pool.Stop()
}

// Abort the multipart upload and stop the worker pool.
func (m *MPUploader) Abort() error {
	err := m.Stop()
	if err != nil {
		return fmt.Errorf("failed to stop worker pool: %w", err)
	}

	err = m.opts.Client.AbortMultipartUpload(m.opts.Context, objcli.AbortMultipartUploadOptions{
		Bucket:   m.opts.Bucket,
		UploadID: m.opts.ID,
		Key:      m.opts.Key,
	})

	return err
}

// Commit the multipart upload and stop the worker pool.
func (m *MPUploader) Commit() error {
	err := m.Stop()
	if err != nil {
		return fmt.Errorf("failed to stop worker pool: %w", err)
	}

	// Sort the parts prior to completion to ensure the correct order; parts will be uploaded in any arbitrary order
	sort.Slice(
		m.opts.Parts,
		func(i, j int) bool { return m.opts.Parts[i].Number < m.opts.Parts[j].Number },
	)

	err = m.opts.Client.CompleteMultipartUpload(m.opts.Context, objcli.CompleteMultipartUploadOptions{
		Bucket:       m.opts.Bucket,
		UploadID:     m.opts.ID,
		Key:          m.opts.Key,
		Parts:        m.opts.Parts,
		Precondition: m.opts.Precondition,
		Lock:         m.opts.Lock,
	})

	return err
}
