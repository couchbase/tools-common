package netutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldReconstructIPV6(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected bool
	}

	tests := []*test{
		{
			name:  "ShouldIgnore",
			input: "hostname",
		},
		{
			name:     "ShouldReconstruct",
			input:    "::1",
			expected: true,
		},
		{
			name:  "ShouldNotReconstruct",
			input: "[::1]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, ShouldReconstructIPV6(test.input))
		})
	}
}
