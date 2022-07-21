package objutil

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/couchbase/tools-common/fsutil"
	"github.com/couchbase/tools-common/hofp"
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objval"
)

// SyncOptions encapsulates all the options available when syncing a directory/object to/from a remote cloud.
type SyncOptions struct {
	Options

	// Client is the client used to perform the operation. If not passed then a default client will be created using the
	// scheme
	//
	// NOTE: Required
	Client objcli.Client

	// Source is where to sync from
	//
	// NOTE: Required
	Source string

	// Destination is where to sync to
	//
	// NOTE: Required
	Destination string

	// MPUThreshold is a threshold at which point objects which broken down into multipart uploads.
	//
	// NOTE: Only used for upload.
	MPUThreshold int64
}

// Sync copies a directory to/from cloud storage from/to a filepath.
//
// Example:
//
//   c := s3.New(session.New())
//   cli := objaws.NewClient(c)
//   Sync(SyncOptions {
//     Client: cli,
//     Source: "/tmp/data/directory",
//     Destination: "s3://bucket-name/sub/path/",
//   })
//
// NOTE: When the filepath has a trailing slash the contents of the directory are up/downloaded, whereas without it the
// directory itself is up/downloaded. As an example given a file test.txt in /tmp/data/ then running Sync with
// SyncOptions{Source: "/tmp/data/", Destination: "s3://bucket/foo/"} will result in s3://bucket/foo/test.txt, whereas
// running with SyncOptions{Source: "/tmp/data", Destination: "s3://bucket/foo/"} will result in
// s3://bucket/foo/data/test.txt
func Sync(opts SyncOptions) error {
	src, dst, err := parseURLs(opts)
	if err != nil {
		return err
	}

	if (isCloudProvider(src) && isCloudProvider(dst)) || (!isCloudProvider(src) && !isCloudProvider(dst)) {
		return fmt.Errorf("one of source and destination needs to be a file path, and the other needs to be a cloud path")
	}

	if isCloudProvider(src) {
		return download(src, dst, opts)
	}

	exists, err := fsutil.DirExists(src.Path)
	if err != nil {
		return fmt.Errorf("could not check if source exists: %w", err)
	}

	if !exists {
		return fmt.Errorf("sync only works on directories, %s is a file", src)
	}

	return uploadDirectory(src, dst, opts)
}

// isCloudProvider checks if u's provider is not None.
func isCloudProvider(u *CloudOrFileURL) bool {
	return u.Provider != objval.ProviderNone
}

// parseURLs parses Source and Destination into CloudOrFileURLs.
func parseURLs(opts SyncOptions) (*CloudOrFileURL, *CloudOrFileURL, error) {
	src, err := ParseCloudOrFileURL(opts.Source)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse source: %w", err)
	}

	dst, err := ParseCloudOrFileURL(opts.Destination)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse destination: %w", err)
	}

	return src, dst, nil
}

// addTrailingPathSeparator adds a trailing path separator to path if it does not have one already.
func addTrailingPathSeparator(path string) string {
	if !strings.HasSuffix(path, string(os.PathSeparator)) {
		return path + string(os.PathSeparator)
	}

	return path
}

// uploadDirectory uploads a directory from the local file system to cloud storage. Assumes source is a file:// URL and
// destination is a cloud-specific one.
func uploadDirectory(source, destination *CloudOrFileURL, opts SyncOptions) error {
	// Add a trailing path separator so when we trim the path in ul below the result is a relative path, not an absolute
	// one
	srcPrefix := addTrailingPathSeparator(source.Path)
	if !strings.HasSuffix(source.Path, string(os.PathSeparator)) {
		srcPrefix = addTrailingPathSeparator(filepath.Dir(source.Path))
	}

	pool := hofp.NewPool(hofp.Options{LogPrefix: "(objutil)"})

	ul := func(path string) error {
		// Get the path relative to the directory we're uploading which we then use as the destination subpath
		var (
			suffix         = strings.TrimPrefix(path, srcPrefix)
			newDestination = destination.Join(suffix)
		)

		newSource, err := ParseCloudOrFileURL(path)
		if err != nil {
			return fmt.Errorf("could not parse file path: %w", err)
		}

		return uploadFile(newSource, newDestination, opts)
	}

	err := filepath.Walk(source.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		return pool.Queue(func() error { return ul(path) })
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
func uploadFile(source, destination *CloudOrFileURL, opts SyncOptions) error {
	file, err := fsutil.OpenRandAccess(source.Path, 0, 0)
	if err != nil {
		return fmt.Errorf("could not open specified file: %w", err)
	}
	defer file.Close()

	log.Debugf("(objutil) Uploading '%s' to '%s'", source, destination)

	return Upload(UploadOptions{
		Client:       opts.Client,
		Bucket:       destination.Bucket,
		Key:          destination.Path,
		Body:         file,
		MPUThreshold: opts.MPUThreshold,
		Options:      Options{PartSize: opts.PartSize},
	})
}

// download downloads all files under the prefix in opts.Source to the given destination. Assumes source is a cloud
// path and destination is a local path to a directory.
//
// NOTE: if you specify a source such as "path/to/dir" then the directory "path/to/dir/" is considered under the source,
// so a "dir" directory will be created under your destination. To avoid this specify your source with a trailing slash.
func download(source, destination *CloudOrFileURL, opts SyncOptions) error {
	destination.Path = addTrailingPathSeparator(destination.Path)

	pool := hofp.NewPool(hofp.Options{LogPrefix: "(objutil)"})

	keyPrefix := path.Dir(source.Path)
	if strings.HasSuffix(source.Path, "/") {
		keyPrefix = source.Path
	}

	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	dl := func(key string) error {
		var (
			newSource      = CloudOrFileURL{Bucket: source.Bucket, Provider: source.Provider, Path: key}
			newDestination = destination.Join(strings.TrimPrefix(key, keyPrefix))
		)

		return downloadFile(&newSource, newDestination, opts)
	}

	err := opts.Client.IterateObjects(source.Bucket, source.Path, "", nil, nil, func(attrs *objval.ObjectAttrs) error {
		if attrs.IsDir() {
			return nil
		}

		return pool.Queue(func() error { return dl(attrs.Key) })
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
func downloadFile(source, destination *CloudOrFileURL, opts SyncOptions) error {
	log.Debugf("(objutil) Downloading '%s' to '%s'", source, destination)

	err := fsutil.Mkdir(filepath.Dir(destination.Path), 0, true, true)
	if err != nil {
		return fmt.Errorf("could not create subdirectories: %w", err)
	}

	file, err := fsutil.CreateFile(destination.Path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("could not open specified file: %w", err)
	}
	defer file.Close()

	return Download(DownloadOptions{
		Client: opts.Client,
		Bucket: source.Bucket,
		Key:    source.Path,
		Writer: file,
		Options: Options{
			PartSize: opts.PartSize,
		},
	})
}
