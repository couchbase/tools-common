//go:build windows
// +build windows

package system

// RaiseFileLimit does nothing on Windows. Windows does not have limits like ulimit. The max number of file handles a
// process can have is 16,744,448.
func RaiseFileLimit(thershold uint64) error {
	return nil
}
