package util

import (
	"os"
)

// Remove the file/directory at the given path.
func Remove(path string, ignoreNotExists bool) error {
	err := os.Remove(path)
	if err == nil || ignoreNotExists && os.IsNotExist(err) {
		return nil
	}

	return err
}

// RemoveAll removes all the files/directories at the provided path.
func RemoveAll(path string) error {
	err := os.RemoveAll(path)
	if err == nil {
		return nil
	}

	return err
}
