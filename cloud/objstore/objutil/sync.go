package objutil

import (
	"fmt"
	"log/slog"

	"golang.org/x/time/rate"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	fsutil "github.com/couchbase/tools-common/fs/util"
)

// SyncOptions encapsulates all the options available when syncing a directory/object to/from a remote cloud.
type SyncOptions struct {
	Options

	// Limiter will rate limit the reads/writes for upload/download.
	Limiter *rate.Limiter

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

	// Logger is the passed Logger struct that implements the Log method for logger the user wants to use.
	Logger *slog.Logger

	// Precondition is used to perform a conditional operation. If the precondition is not satisfied the operation will
	// fail.
	Precondition objcli.OperationPrecondition

	// Lock is the object lock which determines the period during which the object will be immutable. If set to nil the
	// object will be mutable.
	//
	// NOTE: Verify that versioning/locking is enabled using `GetBucketLockingStatus` before setting a lock.
	Lock *objcli.ObjectLock
}

// defaults fills any missing attributes to a sane default.
func (s *SyncOptions) defaults() {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
}

// Sync copies a directory to/from cloud storage from/to a filepath.
//
// Example:
//
//	err = Sync(SyncOptions {
//	  Client: objaws.NewClient(s3.New(session.New())),
//	  Source: "/tmp/data/directory",
//	  Destination: "s3://bucket-name/sub/path/",
//	})
//
// NOTE: When the filepath has a trailing slash the contents of the directory are up/downloaded, whereas without it the
// directory itself is up/downloaded. As an example given a file test.txt in /tmp/data/ then running Sync with
// SyncOptions{Source: "/tmp/data/", Destination: "s3://bucket/foo/"} will result in s3://bucket/foo/test.txt, whereas
// running with SyncOptions{Source: "/tmp/data", Destination: "s3://bucket/foo/"} will result in
// s3://bucket/foo/data/test.txt
func Sync(opts SyncOptions) error {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	src, dst, err := parseURLs(opts.Source, opts.Destination)
	if err != nil {
		return err
	}

	if (isCloudProvider(src) && isCloudProvider(dst)) || (!isCloudProvider(src) && !isCloudProvider(dst)) {
		return fmt.Errorf("one of source and destination needs to be a file path, and the other needs to be a cloud path")
	}

	syncer := NewSyncer(opts)

	if isCloudProvider(src) {
		return syncer.Download(src, dst)
	}

	exists, err := fsutil.DirExists(src.Path)
	if err != nil {
		return fmt.Errorf("could not check if source exists: %w", err)
	}

	if !exists {
		return fmt.Errorf("sync only works on directories, %s is a file", src)
	}

	return syncer.Upload(src, dst)
}

// isCloudProvider checks if u's provider is not None.
func isCloudProvider(u *CloudOrFileURL) bool {
	return u.Provider != objval.ProviderNone
}

// parseURLs parses Source and Destination into CloudOrFileURLs.
func parseURLs(source, destination string) (*CloudOrFileURL, *CloudOrFileURL, error) {
	src, err := ParseCloudOrFileURL(source)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse source: %w", err)
	}

	dst, err := ParseCloudOrFileURL(destination)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse destination: %w", err)
	}

	return src, dst, nil
}
