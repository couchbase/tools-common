package system

import (
	"fmt"
	"regexp"
	"strconv"
)

// totalMemory returns the total physical memory available on the host machine in bytes.
func totalMemory() (uint64, error) {
	output, err := Execute("sysctl hw.memsize")
	if err != nil {
		return 0, err // Purposefully not wrapped
	}

	matches := regexp.MustCompile(`hw\.memsize:\s+(\d+)`).FindStringSubmatch(output)
	if matches == nil {
		return 0, fmt.Errorf("expected to find 'hw.memsize' in 'sysctl hw.memsize' output")
	}

	return strconv.ParseUint(matches[1], 10, 64)
}
