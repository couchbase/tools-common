package system

import (
	"bytes"
	"fmt"
	"os/exec"
)

// Execute will execute the given command in the platforms default shell/command prompt and return the combined
// stdout/stderr output.
//
// NOTE: In the event of an error, the returned string will always be empty, the error message will contain all the
// valid information about why the command failed.
func Execute(command string) (string, error) {
	output, err := exec.Command(shell, append(flags, command)...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run '%s': %s", command, formatCommandError(output, err))
	}

	return string(bytes.TrimSpace(output)), nil
}
