package testutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// MarshalJSON marshals the provided interface to JSON fatally terminating the current test in the event of a failure.
func MarshalJSON(t *testing.T, data interface{}) []byte {
	dJSON, err := json.Marshal(data)
	require.NoError(t, err)

	return dJSON
}
