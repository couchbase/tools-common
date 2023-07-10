package format

import "time"

// Duration truncate the provided duration to a sensible precision and return it formatted as a string.
func Duration(d time.Duration) string {
	if d < time.Minute {
		return d.Truncate(time.Millisecond).String()
	}

	return d.Truncate(time.Second).String()
}
