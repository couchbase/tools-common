package objgcp

// MaxComposable is the hard limit imposed by Google Storage on the maximum number of objects which can be composed into
// one, however, note that composed objects may be used as the source for composed objects.
const MaxComposable = 32
