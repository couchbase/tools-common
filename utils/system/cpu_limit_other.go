//go:build !linux
// +build !linux

package system

import (
	"runtime"
)

// getMaxProcsRespectingLimit returns the value of GOMAXPROCS (on Linux it will find the CPU usage limit if one is
// defined).
func getMaxProcsRespectingLimit() float64 {
	return float64(runtime.GOMAXPROCS(0))
}
