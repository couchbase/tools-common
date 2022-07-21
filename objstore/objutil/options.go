package objutil

import "context"

// Options contains common options for upload/download of objects.
type Options struct {
	// Context is the ctx.Context that can be used to cancel all requests.
	Context context.Context

	// ParseSize is the size in bytes of individual parts in multipart up/download.
	PartSize int64
}
