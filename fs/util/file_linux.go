//go:build linux
// +build linux

package util

import (
	"os"

	"golang.org/x/sys/unix"
)

// OpenSeqAccess opens the provided file whilst advising the kernel that we'll be accessing the byte range from start to
// end sequentially; this should increase the pre-fetch amount for this file. If a zero start/end is provided kernels
// after 2.6.6 will treat this as "all the bytes through to the end of the file".
//
// NOTE: This is advise to the kernel, the kernel provides no guarantees that it will honor/action the advise.
func OpenSeqAccess(path string, start, end int64) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return file, unix.Fadvise(int(file.Fd()), start, end, unix.FADV_SEQUENTIAL)
}

// OpenRandAccess opens the provided file whilst advising the kernel that we'll be accessing the byte range from start
// to end with a random access pattern; this should decrease the pre-fetch amount and avoid bloating the page cache with
// data that we'll only be using once. If a zero start/end is provided kernels after 2.6.6 will treat this as "all the
// bytes through to the end of the file".
//
// NOTE: This is advise to the kernel, the kernel provides no guarantees that it will honor/action the advise.
func OpenRandAccess(path string, start, end int64) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return file, unix.Fadvise(int(file.Fd()), start, end, unix.FADV_RANDOM)
}
