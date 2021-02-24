package format

import (
	"fmt"
	"time"
)

const (
	Kilobyte uint64 = 1024
	Megabyte        = Kilobyte * 1024
	Gigabyte        = Megabyte * 1024
	Terabyte        = Gigabyte * 1024
	Petabyte        = Terabyte * 1024
	Exabyte         = Petabyte * 1024
)

// Bytes will return a string with the bytes formatted as the largest possible unit to two decimal places.
// Note: This function returns formatted bytes as binary bytes so iB (multiple of 1024) and the largest unit is EiB.
func Bytes(bytes uint64) string {
	switch {
	case bytes < Kilobyte:
		return fmt.Sprintf("%dB", bytes)
	case bytes < Megabyte:
		return fmt.Sprintf("%.2fKiB", float64(bytes)/float64(Kilobyte))
	case bytes < Gigabyte:
		return fmt.Sprintf("%.2fMiB", float64(bytes)/float64(Megabyte))
	case bytes < Terabyte:
		return fmt.Sprintf("%.2fGiB", float64(bytes)/float64(Gigabyte))
	case bytes < Petabyte:
		return fmt.Sprintf("%.2fTiB", float64(bytes)/float64(Terabyte))
	case bytes < Exabyte:
		return fmt.Sprintf("%.2fPiB", float64(bytes)/float64(Petabyte))
	default:
		return fmt.Sprintf("%.2fEiB", float64(bytes)/float64(Exabyte))
	}
}

// Duration truncate the provided duration to a sensible precision and return it formatted as a string.
func Duration(d time.Duration) string {
	if d < time.Minute {
		return d.Truncate(time.Millisecond).String()
	}

	return d.Truncate(time.Second).String()
}
