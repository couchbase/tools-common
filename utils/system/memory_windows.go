package system

import (
	"fmt"
	"regexp"
	"strconv"
)

// totalMemory returns the total physical memory available on the host machine in bytes.
func totalMemory() (uint64, error) {
	output, err := Execute("wmic computersystem get TotalPhysicalMemory")
	if err != nil {
		return 0, err // Purposefully not wrapped
	}

	matches := regexp.MustCompile(`TotalPhysicalMemory\s+(\d+)`).FindStringSubmatch(string(output))
	if matches == nil {
		return 0, fmt.Errorf("expected to find 'TotalPhysicalMemory' in 'wmic computersystem get " +
			"TotalPhysicalMemory' output")
	}

	return strconv.ParseUint(matches[1], 10, 64)
}
