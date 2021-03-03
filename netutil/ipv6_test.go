package netutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldReconstructIPV6(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected string
	}

	tests := []*test{
		{
			name:     "ShouldIgnore",
			input:    "hostname",
			expected: "hostname",
		},
		{
			name:     "ShouldReconstruct",
			input:    "::1",
			expected: "[::1]",
		},
		{
			name:     "ShouldNotReconstruct",
			input:    "[::1]",
			expected: "[::1]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, ReconstructIPV6(test.input))
		})
	}
}
