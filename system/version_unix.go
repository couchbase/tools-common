// +build !windows

package system

import (
	"fmt"
	"os/exec"
	"strings"
)

// version returns a string representation of the current kernel release.
func version() (string, error) {
	output, err := exec.Command("uname", "-r").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run 'uname -r': %s", output)
	}

	return strings.TrimSpace(string(output)), nil
}
