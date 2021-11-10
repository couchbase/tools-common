package objerr

import "errors"

// ErrEndpointResolutionFailed is returned if we've failed to resolve the cloud endpoint for some reason.
var ErrEndpointResolutionFailed = errors.New("cloud endpoint domain name resolution failed, " +
	"check account name/region and endpoint are all valid")
