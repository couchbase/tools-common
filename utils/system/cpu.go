// Package system provides system utility functions initially required by `cbbackupmgr`.
package system

import "sync"

var (
	numCPU     int
	numCPUOnce sync.Once
)

// NumCPU returns GOMAXPROCS (defaults to vCPUs) detected by the runtime multiplied by a constant. This function should
// be used when determining how many Goroutines to create for performing short running tasks which benefit from being
// performed concurrently. We currently multiply the value by 0.75 to avoid over-saturating the CPU in cases where
// multiple instances of cbbackupmgr can be run on a single machine e.g. when running info during a merge.
func NumCPU() int {
	numCPUOnce.Do(func() {
		numCPU = max(1, int(getMaxProcsRespectingLimit()*0.75))
	})

	return numCPU
}

// NumWorkers returns a sane number of workers to create when performing a task concurrently. This function should be
// used for the same reason as 'NumCPU', however, with the added caveat that we'd like to ensure we don't create more
// workers than the amount of work that is going to be processed.
func NumWorkers(limit int) int {
	numCPU := NumCPU()
	if numCPU > 1 && limit > 0 {
		numCPU = min(numCPU, limit)
	}

	return numCPU
}
