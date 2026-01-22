package cbcrypto

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testKey   = []byte("0123456789abcdef0123456789abcdef") // 32 bytes for AES-256
	testKeyID = "01234567-89ab-cdef-0123-456789abcdef"
	testData  = []byte("The quick brown fox jumps over the lazy dog.")
)

func TestNewReader(t *testing.T) {
	keyProvider := func(keyID string) ([]byte, error) {
		if keyID == testKeyID || keyID == "" {
			return testKey, nil
		}

		return nil, os.ErrNotExist
	}

	testCases := []string{
		"uncompressed",
		"compressed-snappy",
		"compressed-zlib",
		"compressed-gzip",
		"compressed-zstd",
		"compressed-bzip2",
		"zero-length-key-id",
		"v1-kbkdf",
		"v1-pbkdf",
	}

	for _, name := range testCases {
		t.Run(name, func(t *testing.T) {
			file, err := os.Open(filepath.Join("testdata", name))
			require.NoError(t, err)
			defer file.Close()

			reader, err := NewReader(file, keyProvider)
			require.NoError(t, err)

			decryptedData, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, testData, decryptedData)
		})
	}
}

func TestNewReaderErrorCases(t *testing.T) {
	type testCase struct {
		name          string
		file          string
		data          []byte
		keyProvider   KeyProvider
		expectedError string
	}

	testCases := []testCase{
		{
			name:          "invalid-header",
			file:          "bad-magic",
			expectedError: "does not contain the cbcrypto magic string",
		},
		{
			name:          "key-id-too-long",
			file:          "too-long-key-id",
			expectedError: "key identifier length too large",
		},
		{
			name: "key-provider-error",
			file: "uncompressed",
			keyProvider: func(_ string) ([]byte, error) {
				return nil, fmt.Errorf("provider failed")
			},
			expectedError: "failed to obtain data-encryption-key",
		},
		{
			name: "bad-key-size",
			file: "uncompressed",
			keyProvider: func(_ string) ([]byte, error) {
				return []byte("short"), nil
			},
			expectedError: "failed to create AES cipher",
		},
		{
			name: "unexpected-eof",
			file: "unexpected-eof",
			keyProvider: func(_ string) ([]byte, error) {
				return testKey, nil
			},
			expectedError: "unexpected EOF",
		},
		{
			name: "chunk-too-large",
			file: "chunk-too-large",
			keyProvider: func(_ string) ([]byte, error) {
				return testKey, nil
			},
			expectedError: "failed to read chunk",
		},
		{
			name: "bad-key",
			file: "uncompressed",
			keyProvider: func(_ string) ([]byte, error) {
				return []byte("0123456789abcdef0123456789abcdeF"), nil
			},
			expectedError: "failed to decrypt chunk",
		},
		{
			name: "unsupported-compression",
			file: "unsupported-compression",
			keyProvider: func(_ string) ([]byte, error) {
				return testKey, nil
			},
			expectedError: "unsupported compression algorithm",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var reader io.Reader

			if tc.file != "" {
				file, err := os.Open(filepath.Join("testdata", tc.file))
				require.NoError(t, err)
				defer file.Close()

				reader = file
			} else {
				reader = bytes.NewReader(tc.data)
			}

			// This handles errors that occur during header parsing and setup.
			cryptoReader, err := NewReader(reader, tc.keyProvider)
			if err != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)

				return
			}

			// For errors that occur during chunk processing, the error will come from the Read call.
			_, err = io.ReadAll(cryptoReader)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestValidate(t *testing.T) {
	testCases := []struct {
		name          string
		path          string
		expectedError string
	}{
		{
			name: "valid-header",
			path: filepath.Join("testdata", "uncompressed"),
		},
		{
			name: "zero-length-key-id",
			path: filepath.Join("testdata", "zero-length-key-id"),
		},
		{
			name:          "file-too-small",
			path:          filepath.Join("testdata", "file-too-small"),
			expectedError: "failed to read header",
		},
		{
			name:          "bad-magic",
			path:          filepath.Join("testdata", "bad-magic"),
			expectedError: "does not contain the cbcrypto magic string",
		},
		{
			name:          "unsupported-version",
			path:          filepath.Join("testdata", "unsupported-version"),
			expectedError: "unsupported encrypted cbcrypto file version",
		},
		{
			name:          "key-id-too-long",
			path:          filepath.Join("testdata", "too-long-key-id"),
			expectedError: "key identifier length too large",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.Open(tc.path)
			require.NoError(t, err)
			defer file.Close()

			err = Validate(file)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)

				return
			}

			require.NoError(t, err)
		})
	}
}
