// Package format provides the means to format types into human readable strings.
package format

import (
	"fmt"
)

const (
	kilobyte uint64 = 1024
	megabyte        = kilobyte * 1024
	gigabyte        = megabyte * 1024
	terabyte        = gigabyte * 1024
	petabyte        = terabyte * 1024
	exabyte         = petabyte * 1024
)

// Bytes will return a string with the bytes formatted as the largest possible unit to two decimal places.
//
// Note: This function returns formatted bytes as binary bytes so iB (multiple of 1024) and the largest unit is EiB.
func Bytes(bytes uint64) string {
	switch {
	case bytes < kilobyte:
		return fmt.Sprintf("%dB", bytes)
	case bytes < megabyte:
		return fmt.Sprintf("%.2fKiB", float64(bytes)/float64(kilobyte))
	case bytes < gigabyte:
		return fmt.Sprintf("%.2fMiB", float64(bytes)/float64(megabyte))
	case bytes < terabyte:
		return fmt.Sprintf("%.2fGiB", float64(bytes)/float64(gigabyte))
	case bytes < petabyte:
		return fmt.Sprintf("%.2fTiB", float64(bytes)/float64(terabyte))
	case bytes < exabyte:
		return fmt.Sprintf("%.2fPiB", float64(bytes)/float64(petabyte))
	default:
		return fmt.Sprintf("%.2fEiB", float64(bytes)/float64(exabyte))
	}
}
