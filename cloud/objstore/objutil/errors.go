package objutil

import "errors"

// ErrCopyToSamePrefix is returned if the user provides a destination/source prefix which is the same, within the same
// bucket when using `CopyObjects`.
var ErrCopyToSamePrefix = errors.New("copying to the same prefix within a bucket is not supported")
