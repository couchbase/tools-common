package strutil

import "strings"

// Contains is analgous to 'strings.Contains' but accepts a variable number of strings which will be tested. Returns a
// boolean indicating whether the given string contains any of the provided strings.
func Contains(a string, b ...string) bool {
	for _, c := range b {
		if strings.Contains(a, c) {
			return true
		}
	}

	return false
}
