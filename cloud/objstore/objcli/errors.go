package objcli

import "errors"

var (
	// ErrIncludeAndExcludeAreMutuallyExclusive is returned if the user attempts to supply both the include and exclude
	// arguments to 'IterateObjects' at once.
	ErrIncludeAndExcludeAreMutuallyExclusive = errors.New("include/exclude are mutually exclusive")

	// ErrExpectedNoUploadID is returned if the user has provided an upload id for a client which doesn't generate or
	// require upload ids.
	ErrExpectedNoUploadID = errors.New("received an unexpected upload id, cloud provider doesn't required upload ids")
)
