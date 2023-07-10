package objutil

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/couchbase/tools-common/cloud/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/cloud/objstore/objval"
	"github.com/couchbase/tools-common/core/log"
	"github.com/couchbase/tools-common/strings/format"
	"github.com/couchbase/tools-common/sync/hofp"
	"github.com/couchbase/tools-common/types/freelist"
)

// PartCompleteFunc is called once a part of the zip file has been uploaded. size is the size of the part uploaded.
type PartCompleteFunc func(size int)

// ProgressReportFunc is called every time a file has been fully downloaded during CompressUpload. progress is how far
// into downloading every object with the path prefix we are.
type ProgressReportFunc func(progress float64)

// downloadCompleteFunc is called when an object has been downloaded. size is the number of bytes in the object.
type downloadCompleteFunc func(size int64)

// CompressObjectsOptions specifies options which configure what and how objects are compressed and uploaded.
type CompressObjectsOptions struct {
	Options

	// Client is the objcli.Client to use to download and upload.
	//
	// NOTE: required
	Client objcli.Client

	// PartUploadWorkers is the number of parts to upload at once.
	PartUploadWorkers int

	// PartCompleteCallback is called once a part has been uploaded.
	PartCompleteCallback PartCompleteFunc

	// ProgressReportCallback is called to report how far into the CompressUpload process we are.
	//
	// NOTE: If provided then CompressUpload will calculate the size of all objects with the given prefix before
	// starting the download, which may take some time.
	ProgressReportCallback ProgressReportFunc

	// SourceBucket is the bucket to compress objects from.
	//
	// NOTE: required
	SourceBucket string

	// Prefix is the prefix of objects to compress.
	//
	// NOTE: required
	Prefix string

	// Delimiter is used when iterating through the objects that begin with Prefix.
	Delimiter string

	// Include allows you to include certain keys in the zip by a regular expression.
	Include []*regexp.Regexp

	// Exclude allows you to exclude certain keys from being in the zip by a regular expression.
	Exclude []*regexp.Regexp

	// DestinationBucket is the bucket to upload to.
	//
	// NOTE: required
	DestinationBucket string

	// Destination is the key to give the zip that is uploaded.
	//
	// NOTE: required
	Destination string

	// Logger is the log.Logger we should use for reporting information.
	Logger log.Logger
}

func (o *CompressObjectsOptions) defaults() {
	if o.Context == nil {
		o.Context = context.Background()
	}

	if o.PartSize == 0 {
		o.PartSize = objaws.MinUploadSize
	}

	if o.PartUploadWorkers == 0 {
		// For each worker we create a buffer of PartSize bytes. This means we probably want to keep PartUploadWorkers
		// fairly small, particularly as it is unlikely we will upload many parts at a time given download is likely to be
		// the bottleneck
		//
		// NOTE: We may reconsider this. See https://issues.couchbase.com/browse/MB-53854
		o.PartUploadWorkers = 4
	}
}

// stipPrefix removes prefix from the beginning of full path.
//
// NOTE: if a trailing '/' is provided then the full prefix will be removed. Without it we will retain the last path
// element. For example:
//
// prefix   | fullPath           | return
// ---------+--------------------+---------------
// foo/bar  | foo/bar/baz/01.txt | bar/baz/01.txt
// foo/bar/ | foo/bar/baz/01.txt | baz/01.txt
func stripPrefix(prefix, fullPath string) string {
	endsWithSeparator := strings.HasSuffix(prefix, "/")

	pref := prefix
	if !endsWithSeparator {
		pref += "/"
	}

	res := strings.TrimPrefix(fullPath, pref)
	if endsWithSeparator {
		return res
	}

	return path.Join(path.Base(prefix), res)
}

// download streams the given object and writes it to zipWriter.
func download(
	ctx context.Context, opts CompressObjectsOptions, logger log.WrappedLogger, key string, sz int64,
	zipWriter *zip.Writer,
) error {
	start := time.Now()

	logger.Infof("Starting download of %s", key)

	writer, err := zipWriter.Create(stripPrefix(opts.Prefix, key))
	if err != nil {
		return fmt.Errorf("could not create file in zip: %w", err)
	}

	obj, err := opts.Client.GetObject(ctx, opts.SourceBucket, key, nil)
	if err != nil {
		return fmt.Errorf("could not get object '%s': %w", key, err)
	}
	defer obj.Body.Close()

	_, err = io.Copy(writer, obj.Body)
	if err != nil {
		return fmt.Errorf("could not copy object into zip: %w", err)
	}

	logger.Infof("Downloaded %s (%s bytes) in %s", key, format.Bytes(uint64(sz)), time.Since(start))

	return nil
}

// iterate calls download on each object that matches the iterate parameters given in opts.
func iterate(ctx context.Context, opts CompressObjectsOptions, zipWriter *zip.Writer, cb downloadCompleteFunc) error {
	logger := log.NewWrappedLogger(opts.Logger)

	fn := func(attrs *objval.ObjectAttrs) error {
		if attrs.IsDir() {
			return nil
		}

		if err := download(ctx, opts, logger, attrs.Key, attrs.Size, zipWriter); err != nil {
			return err
		}

		if cb != nil {
			cb(attrs.Size)
		}

		return nil
	}

	err := opts.Client.IterateObjects(ctx,
		opts.SourceBucket,
		opts.Prefix,
		opts.Delimiter,
		opts.Include,
		opts.Exclude,
		fn,
	)
	if err != nil {
		return fmt.Errorf("error whilst iterating objects: %w", err)
	}

	return nil
}

// uploadFromReader reads from reader and sends it using opts.Client to opts.Destination.
//
// NOTE: It does this by keeping opts.PartUploadWorkers internal buffers, reading into those and uploading them as parts
// of the final object. Having multiple buffers means we do not need to wait for a part to be uploaded to start reading
// from reader again.
func uploadFromReader(ctx context.Context, opts CompressObjectsOptions, reader io.ReadCloser) error {
	defer reader.Close()

	fl := freelist.NewFreeListWithFactory(opts.PartUploadWorkers, func() []byte { return make([]byte, opts.PartSize) })

	// payload is the metadata we pass when uploading so that we can give the buffer back to the freelist and call
	// opts.PartCompleteCallback with the correct size.
	type payload struct {
		buf  []byte
		size int
	}

	// onComplete returns the slice to the freelist and calls the user's callback.
	onComplete := func(metadata any, part objval.Part) error {
		payload, _ := metadata.(*payload)
		if err := fl.Put(ctx, payload.buf); err != nil {
			return fmt.Errorf("could not return buffer to freelist: %w", err)
		}

		if opts.PartCompleteCallback != nil {
			opts.PartCompleteCallback(payload.size)
		}

		return nil
	}

	mp, err := NewMPUploader(MPUploaderOptions{
		Client:         opts.Client,
		Bucket:         opts.DestinationBucket,
		Key:            opts.Destination,
		OnPartComplete: onComplete,
		Options:        Options{PartSize: opts.PartSize, Context: ctx},
	})
	if err != nil {
		return fmt.Errorf("could not create uploader: %w", err)
	}
	defer mp.Abort() //nolint:errcheck,wsl

	// Repeatedly fill up parts (slices from the freelist) and upload them until we reach the end of reader
	for {
		slice, err := fl.Get(ctx)
		if err != nil {
			return fmt.Errorf("could not get buffer from freelist: %w", err)
		}

		pos, readErr := io.ReadFull(reader, slice)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return fmt.Errorf("could not read: %w", readErr)
		}

		buf := bytes.NewReader(slice[:pos])

		if err = mp.UploadWithMeta(&payload{buf: slice, size: pos}, buf); err != nil {
			return fmt.Errorf("could not upload part: %w", err)
		}

		// If readErr != nil and we didn't return early it means we hit an EOF
		if readErr != nil {
			break
		}
	}

	if err = mp.Commit(); err != nil {
		return fmt.Errorf("could not commit upload: %w", err)
	}

	return nil
}

// calculateSize calculates the total size of all objects with the given prefix.
func calculateSize(opts CompressObjectsOptions) (int64, error) {
	var (
		total int64
		fn    = func(attrs *objval.ObjectAttrs) error {
			total += attrs.Size
			return nil
		}
	)

	err := opts.Client.IterateObjects(opts.Context,
		opts.SourceBucket,
		opts.Prefix,
		opts.Delimiter,
		opts.Include,
		opts.Exclude,
		fn,
	)
	if err != nil {
		return 0, fmt.Errorf("error whilst iterating objects: %w", err)
	}

	return total, nil
}

// CompressObjects takes an object storage prefix and a destination. It will create a zip in destination and compress
// and upload every object with the given prefix there. Each object will be streamed from cloud storage, through a
// ZipWriter and back to cloud storage.
func CompressObjects(opts CompressObjectsOptions) error {
	if opts.Client == nil || opts.SourceBucket == "" || opts.Prefix == "" || opts.DestinationBucket == "" ||
		opts.Destination == "" {
		return fmt.Errorf("missing required parameters")
	}

	opts.defaults()

	var totalSize int64

	if opts.ProgressReportCallback != nil {
		var err error

		totalSize, err = calculateSize(opts)
		if err != nil {
			return fmt.Errorf("could not calculate size of objects with path prefix: %w", err)
		}
	}

	var (
		r, w      = io.Pipe()
		zipWriter = zip.NewWriter(w)

		bytesDownloaded int64
		fn              func(size int64)

		pool = hofp.NewPool(hofp.Options{Context: opts.Context, Size: 2, Logger: opts.Logger})
	)

	if opts.ProgressReportCallback != nil && totalSize != 0 {
		fn = func(size int64) {
			bytesDownloaded += size

			progress := math.Min(float64(bytesDownloaded)/float64(totalSize), 1.0)
			opts.ProgressReportCallback(progress)
		}
	}

	pool.Queue(func(ctx context.Context) error { //nolint:errcheck
		defer w.Close()
		defer zipWriter.Close()

		if err := iterate(ctx, opts, zipWriter, fn); err != nil {
			return fmt.Errorf("could not iterate through objects: %w", err)
		}

		if err := zipWriter.Close(); err != nil {
			return fmt.Errorf("could not close zip writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("could not close pipe writer: %w", err)
		}

		return nil
	})

	pool.Queue(func(ctx context.Context) error { return uploadFromReader(ctx, opts, r) }) //nolint:errcheck

	return pool.Stop()
}
