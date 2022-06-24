//go:build linux
// +build linux

package system

import (
	"os"
	"runtime"
)

// getMaxProcsRespectingLimit returns the CPU usage limit if one has been defined or the value of GOMAXPROCS
func getMaxProcsRespectingLimit() float64 {
	_, exists := os.LookupEnv("GOMAXPROCS")
	if exists {
		return float64(runtime.GOMAXPROCS(0))
	}

	limit, err := getCGroupCPULimit()
	if err != nil {
		return float64(runtime.GOMAXPROCS(0))
	}

	return limit
}
