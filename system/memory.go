package system

import "sync"

var (
	systemMemoryOnce sync.Once
	systemMemory     uint64
)

// TotalMemory returns the total physical memory available on the host machine in bytes.
//
// NOTE: This function is a wrapper around os specific functions which ensures that we always return the same value and
// only calculate it once.
func TotalMemory() (uint64, error) {
	var err error

	systemMemoryOnce.Do(func() {
		systemMemory, err = totalMemory()
	})

	return systemMemory, err
}
