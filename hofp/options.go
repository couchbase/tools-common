package hofp

import "github.com/couchbase/tools-common/system"

// Options encapsulates the available options which can be used when creating a worker pool.
type Options struct {
	// Size dictates the number of goroutines created to process incoming functions. Defaults to the number of vCPUs.
	Size int

	// LogPrefix is the prefix used when logging errors which occur once teardown has already begun. Defaults to
	// '(HOFP)'.
	LogPrefix string
}

func (o *Options) defaults() {
	if o.Size == 0 {
		o.Size = system.NumCPU()
	}

	if o.LogPrefix == "" {
		o.LogPrefix = "(HOFP)"
	}
}
