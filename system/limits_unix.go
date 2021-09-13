//go:build !windows
// +build !windows

package system

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// RaiseFileLimit will check if the ulimit on open files is bigger or equal to threshold if it is it will return.
// Otherwise it will try to raise it and return true. If it fails it will return false.
func RaiseFileLimit(threshold uint64) error {
	var rLimit unix.Rlimit

	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}

	if rLimit.Cur >= threshold {
		return nil
	}

	if rLimit.Max < threshold {
		return fmt.Errorf("open max file limit (%d) is smaller than required (%d)", rLimit.Max, threshold)
	}

	rLimit.Cur = threshold

	err = unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}

	return nil
}
