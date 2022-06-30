package system

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// totalMemory returns the total physical memory available on the host machine in bytes, or the cgroup limit if one has
// been specified
func totalMemory() (uint64, error) {
	computerMem, err := readMemInfo()
	if err != nil {
		return 0, err
	}

	cGroupLimit, err := getCGroupMemoryLimit()
	if err != nil && !errors.Is(err, errNoLimitSpecified) {
		return 0, err
	}

	if err != nil || cGroupLimit > computerMem || cGroupLimit == 0 {
		return computerMem, nil
	}

	return cGroupLimit, nil
}

func readMemInfo() (uint64, error) {
	meminfo, err := os.ReadFile("/proc/meminfo")
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
