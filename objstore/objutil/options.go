package objutil

// Options contains common options for upload/download of objects.
type Options struct {
	// ParseSize is the size in bytes of individual parts in multipart up/download.
	PartSize int64
}
