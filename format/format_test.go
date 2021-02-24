package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	type test struct {
		name     string
		input    uint64
		expected string
	}

	tests := []*test{
		{
			name:     "LowerByteRange",
			expected: "0B",
		},
		{
			name:     "MidByteRange",
			input:    500,
			expected: "500B",
		},
		{
			name:     "HighByteRange",
			input:    1023,
			expected: "1023B",
		},
		{
			name:     "LowKiBRange",
			input:    1024,
			expected: "1.00KiB",
		},
		{
			name:     "MidKiBRange",
			input:    1024 + 100,
			expected: "1.10KiB",
		},
		{
			name:     "HighKiBRange",
			input:    1024*1024 - 1024,
			expected: "1023.00KiB",
		},
		{
			name:     "LowMiBRange",
			input:    1024 * 1024,
			expected: "1.00MiB",
		},
		{
			name:     "MidMiBRange",
			input:    1024*1024 + 1024*500,
			expected: "1.49MiB",
		},
		{
			name:     "HighMiBRange",
			input:    1024*1024*1024 - 1024*100,
			expected: "1023.90MiB",
		},
		{
			name:     "LowGiBRange",
			input:    1024 * 1024 * 1024,
			expected: "1.00GiB",
		},
		{
			name:     "MidGiBRange",
			input:    1024*1024*1024*2 + 1024*1024*100,
			expected: "2.10GiB",
		},
		{
			name:     "HighGiBRange",
			input:    1024*1024*1024*1024 - 1024*1024*100,
			expected: "1023.90GiB",
		},
		{
			name:     "LowTiBRange",
			input:    1024 * 1024 * 1024 * 1024,
			expected: "1.00TiB",
		},
		{
			name:     "MidTiBRange",
			input:    1024*1024*1024*1024 + 1024*1024*1024*100,
			expected: "1.10TiB",
		},
		{
			name:     "HighTiBRange",
			input:    1024*1024*1024*1024*1024 - 1024*1024*1024*100,
			expected: "1023.90TiB",
		},
		{
			name:     "LowPiBRange",
			input:    1024 * 1024 * 1024 * 1024 * 1024,
			expected: "1.00PiB",
		},
		{
			name:     "MidPiBRange",
			input:    1024*1024*1024*1024*1024 + 1024*1024*1024*1024*500,
			expected: "1.49PiB",
		},
		{
			name:     "HighPiBRange",
			input:    1024*1024*1024*1024*1024*1024 - 1024*1024*1024*1024*500,
			expected: "1023.51PiB",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Bytes(test.input)
			assert.Equal(t, actual, test.expected)
		})
	}
}

func TestDuration(t *testing.T) {
	type test struct {
		input    time.Duration
		expected string
	}

	tests := []*test{
		{
			input:    time.Second + 500*time.Millisecond,
			expected: "1.5s",
		},
		{
			input:    time.Minute + time.Second + 500*time.Millisecond,
			expected: "1m1s",
		},
	}

	for _, test := range tests {
		t.Run(test.input.String(), func(t *testing.T) {
			actual := Duration(test.input)
			assert.Equal(t, actual, test.expected)
		})
	}
}
