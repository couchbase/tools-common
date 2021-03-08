package system

import "sync"

var (
	systemVersionOnce sync.Once
	systemVersion     string
)

// Version returns a string representation of the current kernel release.
//
// NOTE: This function is a wrapper around os specific functions which ensures that we always return the same value and
// only calculate it once.
func Version() (string, error) {
	var err error

	systemVersionOnce.Do(func() {
		systemVersion, err = version()
	})

	return systemVersion, err
}
