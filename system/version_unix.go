// +build !windows

package system

// version returns a string representation of the current kernel release.
func version() (string, error) {
	return Execute("uname -r")
}
