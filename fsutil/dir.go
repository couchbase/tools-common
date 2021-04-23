package fsutil

import (
	"os"
)

// DirExists returns a boolean indicating whether a directory at the provided path exists.
func DirExists(path string) (bool, error) {
	stats, err := os.Stat(path)
	if err != nil {
		return false, ignoreINE(err)
	}

	if !stats.IsDir() {
		return false, ErrNotDir
	}

	return true, nil
}

// Mkdir creates a new directory with the provided mode and may optionally create all parent directories.
//
// NOTE: If a zero value file mode is suppled, the default will be used.
func Mkdir(path string, mode os.FileMode, parents, ignoreExists bool) error {
	if mode == 0 {
		mode = DefaultDirMode
	}

	var err error
	if parents {
		err = os.MkdirAll(path, mode)
	} else {
		err = os.Mkdir(path, mode)
	}

	if err != nil && !(ignoreExists && os.IsExist(err)) {
		return err
	}

	// The directories mode may not be exactly what we provided due to a umask, we should update the permissions to be
	// sure.
	return os.Chmod(path, mode)
}
