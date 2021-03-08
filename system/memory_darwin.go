package system

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
)

// totalMemory returns the total physical memory available on the host machine in bytes.
func totalMemory() (uint64, error) {
	output, err := exec.Command("sysctl", "hw.memsize").CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to run 'sysctl hw.memsize': %s", output)
	}

	matches := regexp.MustCompile(`hw\.memsize:\s+(\d+)`).FindStringSubmatch(string(output))
	if matches == nil {
		return 0, fmt.Errorf("expected to find 'hw.memsize' in 'sysctl hw.memsize' output")
	}

	return strconv.ParseUint(matches[1], 10, 64)
}
