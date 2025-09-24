package cbcrypto

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	keyProvider := func(keyID string) ([]byte, error) {
		if keyID == testKeyID {
			return testKey, nil
		}

		return nil, nil
	}

	type testCase struct {
		name        string
		compression CompressionType
	}

	testCases := []testCase{
		{"uncompressed", None},
		{"compressed-snappy", Snappy},
		{"compressed-zlib", ZLib},
		{"compressed-gzip", GZip},
		{"compressed-zstd", ZStd},
		{"compressed-bzip2", BZip2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			chunk1 := []byte("This is the first chunk of data.")
			chunk2 := []byte("This is the second chunk, appended.")

			writer, err := NewCBCWriter(&buf, tc.compression, testKeyID, testKey)
			require.NoError(t, err)

			require.NoError(t, writer.AppendChunk(bytes.NewReader(chunk1)))
			require.NoError(t, writer.AppendChunk(bytes.NewReader(chunk2)))

			reader, err := NewReader(bytes.NewReader(buf.Bytes()), keyProvider)
			require.NoError(t, err)

			decryptedData, err := io.ReadAll(reader)
			require.NoError(t, err)

			require.Equal(t, bytes.Join([][]byte{chunk1, chunk2}, nil), decryptedData)
		})
	}
}

// failingWriter is a writer that fails on the first write.
type failingWriter struct{}

func (fw *failingWriter) Write(_ []byte) (n int, err error) {
	return 0, errors.New("write failed")
}

func TestWriterErrorCases(t *testing.T) {
	t.Run("create-with-failing-writer", func(t *testing.T) {
		_, err := NewCBCWriter(&failingWriter{}, None, testKeyID, testKey)
		require.ErrorContains(t, err, "failed to write header")
	})

	t.Run("create-bad-key-size", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := NewCBCWriter(&buf, None, testKeyID, []byte("short"))
		require.ErrorContains(t, err, "failed to create AES cipher")
	})
}
