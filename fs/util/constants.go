package util

import "os"

const (
	// DefaultFileMode represents the default file mode which will be used if a zero value file mode is provided for
	// operations which create new files.
	DefaultFileMode os.FileMode = 0o660

	// DefaultDirMode represents the default file mode which will be used if a zero value file mode is provided for
	// operations which create new directories.
	DefaultDirMode os.FileMode = 0o770
)
