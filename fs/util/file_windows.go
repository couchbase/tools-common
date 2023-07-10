//go:build windows
// +build windows

package util

import (
	"os"

	"golang.org/x/sys/windows"
)

// OpenSeqAccess opens the provided file whilst advising the OS that we'll be accessing the file sequentially.
func OpenSeqAccess(path string, _, _ int64) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|windows.FILE_FLAG_SEQUENTIAL_SCAN, 0)
}

// OpenRandAccess opens the provided file whilst advising the OS that we'll be accessing the file with a random access
// pattern.
func OpenRandAccess(path string, _, _ int64) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|windows.FILE_FLAG_RANDOM_ACCESS, 0)
}
