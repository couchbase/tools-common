package system

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
)

// TotalMemory returns the total physical memory available on the host machine in bytes.
func TotalMemory() (uint64, error) {
	output, err := exec.Command("wmic", "computersystem", "get", "TotalPhysicalMemory")
	if err != nil {
		return 0, fmt.Errorf("failed to run 'wmic computersystem get TotalPhysicalMemory': %s", output)
	}

	matches := regexp.MustCompile(`TotalPhysicalMemory\s+(\d+)`).FindStringSubmatch(string(output))
	if matches == nil {
		return 0, fmt.Errorf("expected to find 'TotalPhysicalMemory' in 'wmic computersystem get " +
			"TotalPhysicalMemory' output")
	}

	return strconv.ParseUint(matches[1], 10, 64)
}
