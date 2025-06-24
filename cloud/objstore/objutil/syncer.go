package objutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	fsutil "github.com/couchbase/tools-common/fs/util"
	"github.com/couchbase/tools-common/sync/v2/hofp"
	ioiface "github.com/couchbase/tools-common/types/v2/iface"
	"github.com/couchbase/tools-common/types/v2/ratelimit"
)

// Syncer exposes the ability to sync files and directories to/from a remote cloud provider.
type Syncer struct {
	opts   SyncOptions
	logger *slog.Logger
}

// NewSyncer creates a new syncer using the given options.
func NewSyncer(opts SyncOptions) *Syncer {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	syncer := Syncer{
		opts:   opts,
		logger: opts.Logger,
	}

	return &syncer
}

// addTrailingPathSeparator adds a trailing path separator to path if it does not have one already.
func (s *Syncer) addTrailingPathSeparator(path string) string {
	if !strings.HasSuffix(path, string(os.PathSeparator)) {
		return path + string(os.PathSeparator)
	}

	return path
}

// Upload a directory from the local file system to cloud storage. Assumes source is a file:// URL and destination is a
// cloud-specific one.
func (s *Syncer) Upload(source, destination *CloudOrFileURL) error {
	// Add a trailing path separator so when we trim the path in ul below the result is a relative path, not an absolute
	// one
	srcPrefix := s.addTrailingPathSeparator(source.Path)
	if !strings.HasSuffix(source.Path, string(os.PathSeparator)) {
		srcPrefix = s.addTrailingPathSeparator(filepath.Dir(source.Path))
	}

	pool := hofp.NewPool(hofp.Options{
		Context: s.opts.Context,
	})

	ul := func(ctx context.Context, path string) error {
		// Get the path relative to the directory we're uploading which we then use as the destination subpath
		var (
			suffix         = strings.TrimPrefix(path, srcPrefix)
			newDestination = destination.Join(suffix)
		)

		newSource, err := ParseCloudOrFileURL(path)
		if err != nil {
			return fmt.Errorf("could not parse file path: %w", err)
		}

		return s.uploadFile(ctx, newSource, newDestination)
	}

	err := filepath.Walk(source.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		return pool.Queue(func(ctx context.Context) error { return ul(ctx, path) })
	})
	if err != nil {
		return fmt.Errorf("could not walk directory: %w", err)
	}

	err = pool.Stop()
	if err != nil {
		return fmt.Errorf("error whilst uploading directory: %w", err)
	}

	return nil
}

// uploadFile uploads a file to the given cloud provider. Assumes source is a file:// URL to a file, and
// destination is a cloud path.
func (s *Syncer) uploadFile(ctx context.Context, source, destination *CloudOrFileURL) error {
	s.logger.Debug("uploading file", "source", source, "destination", destination)

	file, err := fsutil.OpenRandAccess(source.Path, 0, 0)
	if err != nil {
		return fmt.Errorf("could not open specified file: %w", err)
	}
	defer file.Close()

	var reader ioiface.ReadAtSeeker = file
	if s.opts.Limiter != nil {
		reader = ratelimit.NewRateLimitedReadAtSeeker(ctx, reader, s.opts.Limiter)
	}

	opts := UploadOptions{
		Options:      s.opts.Options.WithContext(ctx),
		Client:       s.opts.Client,
		Bucket:       destination.Bucket,
		Key:          destination.Path,
		Body:         reader,
		MPUThreshold: s.opts.MPUThreshold,
		Precondition: s.opts.Precondition,
		Lock:         s.opts.Lock,
	}

	return Upload(opts)
}

// Download all files under the prefix in opts.Source to the given destination. Assumes source is a cloud path and
// destination is a local path to a directory.
//
// NOTE: If you specify a source such as "path/to/dir" then the directory "path/to/dir/" is considered under the source,
// so a "dir" directory will be created under your destination. To avoid this specify your source with a trailing slash.
func (s *Syncer) Download(source, destination *CloudOrFileURL) error {
	destination.Path = s.addTrailingPathSeparator(destination.Path)

	pool := hofp.NewPool(hofp.Options{
		Context: s.opts.Context,
	})

	keyPrefix := path.Dir(source.Path)
	if strings.HasSuffix(source.Path, "/") {
		keyPrefix = source.Path
	}

	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	dl := func(ctx context.Context, key string) error {
		var (
			newSource      = CloudOrFileURL{Bucket: source.Bucket, Provider: source.Provider, Path: key}
			newDestination = destination.Join(strings.TrimPrefix(key, keyPrefix))
		)

		return s.downloadFile(ctx, &newSource, newDestination)
	}

	queue := func(attrs *objval.ObjectAttrs) error {
		if attrs.IsDir() {
			return nil
		}

		return pool.Queue(func(ctx context.Context) error { return dl(ctx, attrs.Key) })
	}

	err := s.opts.Client.IterateObjects(s.opts.Context, objcli.IterateObjectsOptions{
		Bucket: source.Bucket,
		Prefix: source.Path,
		Func:   queue,
	})
	if err != nil {
		return fmt.Errorf("could not iterate objects: %w", err)
	}

	err = pool.Stop()
	if err != nil {
		return fmt.Errorf("error whilst downloading: %w", err)
	}

	return nil
}

// downloadFile downloads a file in the cloud to a file on disk. Assumes source is a cloud URL to an object and
// destination is a file:// URL to a file.
func (s *Syncer) downloadFile(ctx context.Context, source, destination *CloudOrFileURL) error {
	s.logger.Debug("downloading file", "source", source, "destination", destination)

	err := fsutil.Mkdir(filepath.Dir(destination.Path), 0, true, true)
	if err != nil {
		return fmt.Errorf("could not create subdirectories: %w", err)
	}

	file, err := fsutil.CreateFile(destination.Path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("could not open specified file: %w", err)
	}
	defer file.Close()

	var writer io.WriterAt = file
	if s.opts.Limiter != nil {
		writer = ratelimit.NewRateLimitedWriterAt(ctx, writer, s.opts.Limiter)
	}

	opts := DownloadOptions{
		Options: s.opts.Options.WithContext(ctx),
		Client:  s.opts.Client,
		Bucket:  source.Bucket,
		Key:     source.Path,
		Writer:  writer,
	}

	return Download(opts)
}
