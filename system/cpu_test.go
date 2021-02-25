package system

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumCPU(t *testing.T) {
	numCPU, numVCPU := NumCPU(), runtime.NumCPU()
	if numCPU < 1 || numCPU > numVCPU {
		t.Fatalf("Received an unexpected value from NumCPU, expected a sane value between 1 and %d", numVCPU)
	}
}

func TestNumWorkers(t *testing.T) {
	numWorkers, numVCPU := NumWorkers(0), runtime.NumCPU()
	if numWorkers < 1 || numWorkers > numVCPU {
		t.Fatalf("Received an unexpected value from NumCPU, expected a sane value between 1 and %d", numVCPU)
	}

	require.Equal(t, NumCPU(), numWorkers, "With a zero value limit, NumWorkers should be equivalent to NumCPU")
	require.Equal(t, 1, NumWorkers(1))
}
