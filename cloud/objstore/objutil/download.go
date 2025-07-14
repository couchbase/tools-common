package objutil

import (
	"io"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
)

// DownloadOptions encapsulates the options available when using the 'Download' function to download data from a remote
// cloud.
type DownloadOptions struct {
	Options

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

	// VersionID is used to identify a specific version when object versioning is enabled.
	VersionID string

	// ByteRange to download from the object.
	//
	// NOTE: Download will not create sparse files, a non-zero start offset will be "shifted" prior to being written to
	// disk.
	ByteRange *objval.ByteRange

	// Writer is the destination for the object.
	//
	// NOTE: The given write must be thread safe.
	Writer io.WriterAt
}

// Download an object from a remote cloud by breaking it up and downloading it in multiple chunks concurrently.
func Download(opts DownloadOptions) error {
	return NewMPDownloader(MPDownloaderOptions(opts)).Download()
}
