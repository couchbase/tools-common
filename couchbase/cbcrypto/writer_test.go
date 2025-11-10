package cbcrypto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
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

func TestOpen(t *testing.T) {
	keyProvider := func(keyID string) ([]byte, error) {
		if keyID == testKeyID {
			return testKey, nil
		}

		return nil, fmt.Errorf("unknown key ID: %s", keyID)
	}

	f, err := os.CreateTemp("", "cbcrypto-test-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	chunk1 := []byte("This is the first chunk of data.")
	chunk2 := []byte("This is the second chunk, appended after opening.")

	writer, err := NewCBCWriter(f, None, testKeyID, testKey)
	require.NoError(t, err)
	require.NoError(t, writer.AppendChunk(bytes.NewReader(chunk1)))

	openedWriter, err := Open(f, testKey)
	require.NoError(t, err)
	require.NoError(t, openedWriter.AppendChunk(bytes.NewReader(chunk2)))

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	reader, err := NewReader(f, keyProvider)
	require.NoError(t, err)
	decryptedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, append(chunk1, chunk2...), decryptedData)
}

// failingWriter is a writer that fails on the first write.
type failingWriter struct{}

func (fw *failingWriter) Write(_ []byte) (n int, err error) {
	return 0, errors.New("write failed")
}

// failingReadWriteSeeker is a ReadWriteSeeker that fails on the first Read.
type failingReadWriteSeeker struct{}

func (f *failingReadWriteSeeker) Read(_ []byte) (n int, err error) {
	return 0, errors.New("read failed")
}

func (f *failingReadWriteSeeker) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (f *failingReadWriteSeeker) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

// writerThatFailsOnNthWrite is a writer that fails on the nth write.
type writerThatFailsOnNthWrite struct {
	n     int
	count int
}

func (w *writerThatFailsOnNthWrite) Write(p []byte) (int, error) {
	if w.count == w.n {
		return 0, errors.New("write failed")
	}

	w.count++

	return len(p), nil
}

func TestWriterErrorCases(t *testing.T) {
	t.Run("create-with-failing-writer", func(t *testing.T) {
		_, err := NewCBCWriter(&failingWriter{}, None, testKeyID, testKey)
		require.ErrorContains(t, err, "failed to write header")
	})

	t.Run("create-bad-key-size", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := NewCBCWriter(&buf, None, testKeyID, make([]byte, 16))
		require.ErrorContains(t, err, "key must be 32 bytes")
	})

	t.Run("create-key-id-too-long", func(t *testing.T) {
		longKeyID := "1234567890123456789012345678901234567"
		_, err := NewCBCWriter(bytes.NewBuffer([]byte{}), None, longKeyID, testKey)
		require.ErrorContains(t, err, "key ID cannot be longer than 36 bytes")
	})

	t.Run("append-chunk-with-failing-writer", func(t *testing.T) {
		writer, err := NewCBCWriter(&writerThatFailsOnNthWrite{n: 1}, None, testKeyID, testKey)
		require.NoError(t, err)

		err = writer.AppendChunk(bytes.NewReader([]byte("some data")))
		require.ErrorContains(t, err, "write failed")
	})
}

func TestOpenErrorCases(t *testing.T) {
	t.Run("open-with-failing-readwriter", func(t *testing.T) {
		_, err := Open(&failingReadWriteSeeker{}, testKey)
		require.ErrorContains(t, err, "failed to read header")
	})

	t.Run("open-bad-key-size", func(t *testing.T) {
		f, err := os.CreateTemp("", "cbcrypto-test-")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		writer, err := NewCBCWriter(f, None, testKeyID, testKey)
		require.NoError(t, err)
		require.NoError(t, writer.AppendChunk(bytes.NewReader([]byte("some data"))))

		_, err = Open(f, make([]byte, 16))
		require.ErrorContains(t, err, "key must be 32 bytes")
	})

	t.Run("open-invalid-file", func(t *testing.T) {
		f, err := os.CreateTemp("", "cbcrypto-test-")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		_, err = Open(f, testKey)
		require.ErrorContains(t, err, "failed to read header")
	})
}
