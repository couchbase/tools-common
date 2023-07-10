//go:build !linux && !windows
// +build !linux,!windows

package util

import "os"

// OpenSeqAccess is a fallback function which will be run if no OS specific function exists; in this case it's no
// different from opening the file normally.
func OpenSeqAccess(path string, _, _ int64) (*os.File, error) {
	return os.Open(path)
}

// OpenRandAccess is a fallback function which will be run if no OS specific function exists; in this case it's no
// different from opening the file normally.
func OpenRandAccess(path string, _, _ int64) (*os.File, error) {
	return os.Open(path)
}
