package system

import "bytes"

// formatCommandError returns a human readable string indicating why we failed to execute a command.
//
// NOTE: output should be the combined output i.e. including stderr
func formatCommandError(output []byte, err error) string {
	if len(output) != 0 {
		return string(bytes.TrimSpace(output))
	}

	return err.Error()
}
