package system

import (
	"fmt"
	"os/exec"
	"strings"
)

// version returns a string representation of the current Windows release.
func version() (string, error) {
	output, err := exec.Command("ver").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run 'ver': %s", output)
	}

	return strings.TrimSpace(string(output)), nil
}
