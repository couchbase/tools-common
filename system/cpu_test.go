package system

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumCPU(t *testing.T) {
	runCPUTest(func() {
		numCPU, numVCPU := NumCPU(), runtime.GOMAXPROCS(0)
		require.False(t, numCPU < 1 || numCPU > numVCPU)
	})
}

func TestNumCPUWithGOMAXPROCS(t *testing.T) {
	runCPUTest(func() {
		old := runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(old)

		numCPU, numVCPU := NumCPU(), runtime.GOMAXPROCS(0)
		require.False(t, numCPU < 1 || numCPU > numVCPU)
	})
}

func TestNumWorkers(t *testing.T) {
	runCPUTest(func() {
		numWorkers, numVCPU := NumWorkers(0), runtime.GOMAXPROCS(0)
		require.False(t, numWorkers < 1 || numWorkers > numVCPU)
		require.Equal(t, NumCPU(), numWorkers, "With a zero value limit, NumWorkers should be equivalent to NumCPU")
		require.Equal(t, 1, NumWorkers(1))
	})
}

// runCPUTest runs the given test whilst performing any global cleanup.
func runCPUTest(fn func()) {
	numCPU = 0
	numCPUOnce = sync.Once{}

	fn()
}
