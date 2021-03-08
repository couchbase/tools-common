package system

import (
	"fmt"
	"os/exec"
	"strings"
)

// Version returns a string representation of the current kernel release.
func Version() (string, error) {
	output, err := exec.Command("uname", "-r").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run 'uname -r': %s", output)
	}

	return strings.TrimSpace(string(output)), nil
}
