package system

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
)

// totalMemory returns the total physical memory available on the host machine in bytes.
func totalMemory() (uint64, error) {
	meminfo, err := ioutil.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("failed to read '/proc/meminfo': %w", err)
	}

	lines := strings.Split(string(meminfo), "\n")
	if len(lines) <= 0 {
		return 0, fmt.Errorf("file '/proc/meminfo' was empty")
	}

	matches := regexp.MustCompile(`MemTotal:\s+(\d+)\s+kB`).FindStringSubmatch(lines[0])
	if matches == nil {
		return 0, fmt.Errorf("expected to find 'MemTotal' in '/proc/meminfo' output")
	}

	total, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse total memory: %w", err)
	}

	return total * 1024, nil
}
