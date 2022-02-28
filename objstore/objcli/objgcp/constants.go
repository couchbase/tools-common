package objgcp

import "time"

const (
	// MaxComposable is the hard limit imposed by Google Storage on the maximum number of objects which can be composed
	// into one, however, note that composed objects may be used as the source for composed objects.
	MaxComposable = 32

	// ChunkSize is the size used for a "resumable" upload in the GCP SDK, required to enable request retries.
	ChunkSize = 5 * 1024 * 1024

	// ChunkRetryDeadline is the timeout for uploading a single chunk to GCP, this matches the timeout used in
	// 'cbbackupmgr' for the object storage HTTP client timeout.
	ChunkRetryDeadline = 30 * time.Minute
)
