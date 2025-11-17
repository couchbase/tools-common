package util

import (
	"fmt"
	"io"
)

// Close and drain the given response body.
//
// NOTE: This should be used by clients, to ensure that the default HTTP transport can reuse connections.
func Close(r io.ReadCloser) error {
	if r == nil {
		return nil
	}

	_, err := io.Copy(io.Discard, r)
	if err != nil {
		return fmt.Errorf("failed to drain body: %w", err)
	}

	err = r.Close()
	if err != nil {
		return fmt.Errorf("failed to close body: %w", err)
	}

	return nil
}
