package batcher

// Option configures a batcher.
type Option func(*options)

// options holds a batcher's optional behaviour.
type options struct {
	// split reports whether a failed batch is run again one item at a time.
	split SplitFunc
}

// newOptions returns the default options, with the given options applied.
func newOptions(opts ...Option) options {
	// Splitting multiplies the calls, so it's off unless asked for
	o := options{
		split: func(error) bool { return false },
	}

	for _, opt := range opts {
		opt(&o)
	}

	return o
}

// WithSplit runs a failed batch again one item at a time when split reports its error. By default, or when split is
// nil, no batch is split, so every item gets the batch's error.
func WithSplit(split SplitFunc) Option {
	return func(o *options) {
		if split == nil {
			return
		}

		o.split = split
	}
}
