package parse

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytes(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected uint64
		err      bool
	}

	tests := []*test{
		{
			name:     "BytesUpper",
			input:    "1024B",
			expected: 1024,
		},
		{
			name:     "KilobytesUpper",
			input:    "1KB",
			expected: 1000,
		},
		{
			name:     "KibibytesUpper",
			input:    "1KiB",
			expected: 1024,
		},
		{
			name:     "MegabytesUpper",
			input:    "1MB",
			expected: 1000 * 1000,
		},
		{
			name:     "MebibytesUpper",
			input:    "1MiB",
			expected: 1024 * 1024,
		},
		{
			name:     "GigabyteUpper",
			input:    "1GB",
			expected: 1000 * 1000 * 1000,
		},
		{
			name:     "GigibyteUpper",
			input:    "1GiB",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "TerabyteUpper",
			input:    "1TB",
			expected: 1000 * 1000 * 1000 * 1000,
		},
		{
			name:     "TebibyteUpper",
			input:    "1TiB",
			expected: 1024 * 1024 * 1024 * 1024,
		},
		{
			name:     "PetabyteUpper",
			input:    "1PB",
			expected: 1000 * 1000 * 1000 * 1000 * 1000,
		},
		{
			name:     "PebibyteUpper",
			input:    "1PiB",
			expected: 1024 * 1024 * 1024 * 1024 * 1024,
		},
		{
			name:     "ExabyteUpper",
			input:    "1EB",
			expected: 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
		},
		{
			name:     "ExbibyteUpper",
			input:    "1EiB",
			expected: 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
	}

	for _, def := range tests {
		tests = append(tests, &test{
			name:     strings.Replace(def.name, "Upper", "Lower", 1),
			input:    strings.ToLower(def.input),
			expected: def.expected,
		})
	}

	tests = append(tests, []*test{
		{
			name:     "Bytes",
			input:    "1024",
			expected: 1024,
		},
		{
			name:     "Float",
			input:    "1.75KiB",
			expected: 1.75 * 1024,
		},
		{
			name:     "FloatWithTrailing",
			input:    "90.0000000005GiB",
			expected: uint64(90.0000000005 * math.Pow(1024, 3)), //nolint:staticcheck
		},
		{
			name:     "WithSpace",
			input:    "50 KiB",
			expected: 50 * 1024,
		},
		{
			name:  "IncompleteSuffix",
			input: "100.5ib",
			err:   true,
		},
		{
			name:  "AnotherIncompleteSuffix",
			input: "100K",
			err:   true,
		},
		{
			name:  "Emoji",
			input: "😊",
			err:   true,
		},
	}...)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := Bytes(test.input)

			if test.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}
