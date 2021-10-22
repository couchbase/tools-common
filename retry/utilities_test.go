package retry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFibN(t *testing.T) {
	type test struct {
		name     string
		n        int
		expected uint64
	}

	tests := []*test{
		{
			name:     "Zero",
			expected: 0,
		},
		{
			name:     "First",
			n:        1,
			expected: 1,
		},
		{
			name:     "Second",
			n:        2,
			expected: 1,
		},
		{
			name:     "Third",
			n:        3,
			expected: 2,
		},
		{
			name:     "FortySecond",
			n:        42,
			expected: 267_914_296,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, fibN(test.n))
		})
	}
}
