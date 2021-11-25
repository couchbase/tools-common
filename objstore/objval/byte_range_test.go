package objval

import (
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/stretchr/testify/require"
)

func TestByteRangeValid(t *testing.T) {
	type test struct {
		name     string
		br       *ByteRange
		required bool
		valid    bool
	}

	tests := []*test{
		{
			name:     "RequiredNotProvided",
			required: true,
		},
		{
			name:  "NotRequiredNotProvided",
			valid: true,
		},
		{
			name:  "ProvidedValid",
			br:    &ByteRange{Start: 64, End: 128},
			valid: true,
		},
		{
			name: "ProvidedNotValidStartBeforeEnd",
			br:   &ByteRange{Start: 128, End: 64},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.valid, test.br.Valid(test.required) == nil)
		})
	}
}

func TestByteRangeToOffsetLength(t *testing.T) {
	type test struct {
		name           string
		input          *ByteRange
		defaultLength  int64
		offset, length int64
	}

	tests := []*test{
		{
			name:          "BothStartAndEnd",
			input:         &ByteRange{Start: 64, End: 128},
			defaultLength: 256, // Should be ignored
			offset:        64,
			length:        65,
		},
		{
			name:          "GCPCountToEnd",
			input:         &ByteRange{Start: 64},
			defaultLength: -1,
			offset:        64,
			length:        -1,
		},
		{
			name:          "AzureCountToEnd",
			input:         &ByteRange{Start: 64},
			defaultLength: azblob.CountToEnd,
			offset:        64,
			length:        azblob.CountToEnd,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			offset, length := test.input.ToOffsetLength(test.defaultLength)
			require.Equal(t, test.offset, offset)
			require.Equal(t, test.length, length)
		})
	}
}

func TestByteRangeToRangeHeader(t *testing.T) {
	type test struct {
		name     string
		input    *ByteRange
		expected string
	}

	tests := []*test{
		{
			name:     "BothStartAndEnd",
			input:    &ByteRange{Start: 64, End: 128},
			expected: "bytes=64-128",
		},
		{
			name:     "JustStart",
			input:    &ByteRange{Start: 64},
			expected: "bytes=64-",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input.ToRangeHeader()
			require.Equal(t, test.expected, actual)
		})
	}
}
