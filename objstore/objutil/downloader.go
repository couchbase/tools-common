package objutil

import (
	"context"
	"fmt"
	"io"

	"github.com/couchbase/tools-common/hofp"
	"github.com/couchbase/tools-common/maths"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objval"
)

// MPDownloaderOptions encapsulates the options available when creating a 'MPDownloader'.
type MPDownloaderOptions struct {
	// Client is the client used to perform the operation.
	//
	// NOTE: This attribute is required.
	Client objcli.Client

	// Bucket is the bucket to download the object from.
	//
	// NOTE: This attribute is required.
	Bucket string

	// Key is the key for the object being downloaded.
	//
	// NOTE: This attribute is required.
	Key string

	// ByteRange to download from the object.
	//
	// NOTE: Download will not create sparse files, a non-zero start offset will be "shifted" prior to being written to
	// disk.
	ByteRange *objval.ByteRange

	// Writer is the destination for the object.
	//
	// NOTE: The given write must be thread safe.
	Writer io.WriterAt

	// PartSize indicates what size should be used for individual parts when being downloaded.
	PartSize int64
}

// defaults populates the options with sensible defaults.
func (m *MPDownloaderOptions) defaults() {
	m.PartSize = maths.MaxInt64(m.PartSize, MinPartSize)
}

// MPDownloader is a multipart downloader which downloads an object from a remote cloud by performing multiple requests
// concurrently using a worker pool.
type MPDownloader struct {
	opts MPDownloaderOptions
}

// NewMPDownloader creates a new multipart downloader using the given objects.
func NewMPDownloader(opts MPDownloaderOptions) *MPDownloader {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	return &MPDownloader{opts: opts}
}

// Download executes the download.
//
// NOTE: If no byte range is provided, the whole object will be downloaded.
func (m *MPDownloader) Download() error {
	br, err := m.byteRange()
	if err != nil {
		return fmt.Errorf("failed to get object byte range: %w", err)
	}

	return m.download(br)
}

// byteRange returns the byte range which should be downloaded.
func (m *MPDownloader) byteRange() (*objval.ByteRange, error) {
	// Provided with a byte range, use this instead of fetching the whole object
	if m.opts.ByteRange != nil {
		return m.opts.ByteRange, nil
	}

	attrs, err := m.opts.Client.GetObjectAttrs(m.opts.Bucket, m.opts.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object attributes: %w", err)
	}

	return &objval.ByteRange{End: attrs.Size - 1}, nil
}

// download the given byte range using multiple concurrent requests.
func (m *MPDownloader) download(br *objval.ByteRange) error {
	pool := hofp.NewPool(hofp.Options{LogPrefix: "(objutil)"})

	queue := func(br *objval.ByteRange) error {
		return pool.Queue(func(_ context.Context) error { return m.downloadChunk(br) })
	}

	for s, e := br.Start, m.opts.PartSize-1; s <= br.End; s, e = s+m.opts.PartSize, e+m.opts.PartSize {
		// Can ignore this error, the same error will be propagated by the call to 'Stop' below.
		if err := queue(&objval.ByteRange{Start: s, End: maths.MinInt64(e, br.End)}); err != nil {
			break
		}
	}

	err := pool.Stop()
	if err != nil {
		return fmt.Errorf("failed to stop worker pool: %w", err)
	}

	return nil
}

// downloadChunk downloads the given byte range and writes it to the underlying write.
func (m *MPDownloader) downloadChunk(br *objval.ByteRange) error {
	object, err := m.opts.Client.GetObject(m.opts.Bucket, m.opts.Key, br)
	if err != nil {
		return fmt.Errorf("failed to get object range: %w", err)
	}

	// The 'WriteAt' interface only allows to to write from a slice, and not a reader so unfortunately this must be read
	// entirely into memory then copied to the destination.
	data, err := io.ReadAll(object.Body)
	if err != nil {
		return fmt.Errorf("failed to read object body: %w", err)
	}

	// Account for the fact that the caller might have requested a non-zero start byte, this has the effect of shifting
	// the start offset towards zero so we don't end up creating a sparse file with a big zero chunk at the beginning.
	var offset int64
	if m.opts.ByteRange != nil {
		offset = m.opts.ByteRange.Start
	}

	_, err = m.opts.Writer.WriteAt(data, br.Start-offset)
	if err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	return nil
}
