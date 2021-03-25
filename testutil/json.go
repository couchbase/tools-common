package testutil

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// MarshalJSON marshals the provided interface to JSON fatally terminating the current test in the event of a failure.
func MarshalJSON(t *testing.T, data interface{}) []byte {
	dJSON, err := json.Marshal(data)
	require.NoError(t, err)

	return dJSON
}

// EncodeJSON marshals then writes the provided interface to the given writer fatally terminating the current test in
// the event of a failure.
func EncodeJSON(t *testing.T, writer io.Writer, data interface{}) {
	require.NoError(t, json.NewEncoder(writer).Encode(data))
}

// UnmarshalJSON unmarshals the provide JSON data into the given interface fatally terminating the current test in the
// even of a failure.
func UnmarshalJSON(t *testing.T, dJSON []byte, data interface{}) {
	err := json.Unmarshal(dJSON, data)
	require.NoError(t, err)
}

// DecodeJSON decodes data from the provided reader into the given interface fatally terminating the current test in the
// event of a failure.
func DecodeJSON(t *testing.T, reader io.Reader, data interface{}) {
	require.NoError(t, json.NewDecoder(reader).Decode(&data))
}
