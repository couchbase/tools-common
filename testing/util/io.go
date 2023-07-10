package util

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Write the given data to the provided writer fatally terminating the current test in the event of a failure.
func Write(t *testing.T, writer io.Writer, data []byte) {
	n, err := writer.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
}

// Read len(data) bytes from the provided reader fatally terminating the current test in the even of a failure.
func Read(t *testing.T, reader io.Reader, data []byte) {
	n, err := io.ReadFull(reader, data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
}

// ReadAll the data from the provided reader fatally terminating the current test in the even of a failure.
func ReadAll(t *testing.T, reader io.Reader) []byte {
	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	return data
}
