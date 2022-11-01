package fsutil

import (
	"os"
)

// Remove the file/directory at the given path.
//
// NOTE: Logs in the event of an error since it's common to ignore the returned error.
func Remove(path string, ignoreNotExists bool) error {
	err := os.Remove(path)
	if err == nil || ignoreNotExists && os.IsNotExist(err) {
		return nil
	}

	return err
}

// RemoveAll removes all the files/directories at the provided path.
//
// NOTE: Logs in the event of an error since it's common to ignore the returned error.
func RemoveAll(path string) error {
	err := os.RemoveAll(path)
	if err == nil {
		return nil
	}

	return err
}
