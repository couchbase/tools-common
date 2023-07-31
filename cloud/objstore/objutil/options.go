package objutil

import (
	"context"

	"github.com/couchbase/tools-common/utils/maths"
)

// Options contains common options for upload/download of objects.
type Options struct {
	// Context is the 'context.Context' that can be used to cancel all requests.
	Context context.Context

	// ParseSize is the size in bytes of individual parts in multipart up/download.
	PartSize int64
}

// defaults fills any missing attributes to a sane default.
func (o *Options) defaults() {
	if o.Context == nil {
		o.Context = context.Background()
	}

	o.PartSize = maths.Max(o.PartSize, MinPartSize)
}

// WithContext returns a copy of the options using the given context.
func (o Options) WithContext(ctx context.Context) Options {
	cp := o
	cp.Context = ctx

	return cp
}
