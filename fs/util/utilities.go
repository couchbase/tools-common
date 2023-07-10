package util

import "os"

// ignoreINE is a utility function which simply cleans up the return path for 'FileExists' and 'DirExists' functions.
func ignoreINE(err error) error {
	if os.IsNotExist(err) {
		return nil
	}

	return err
}
