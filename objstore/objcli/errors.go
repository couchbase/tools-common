package objcli

import "errors"

// ErrIncludeAndExcludeAreMutuallyExclusive is returned if the user attempts to supply both the include and exclude
// arguments to 'IterateObjects' at once.
var ErrIncludeAndExcludeAreMutuallyExclusive = errors.New("include/exclude are mutually exclusive")
