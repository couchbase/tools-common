package slice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEqualStrings(t *testing.T) {
	type test struct {
		name     string
		a, b     []string
		expected bool
	}

	tests := []*test{
		{
			name:     "EmptySlices",
			a:        make([]string, 0),
			b:        make([]string, 0),
			expected: true,
		},
		{
			name:     "EmptySlicesAIsNil",
			b:        make([]string, 0),
			expected: true,
		},
		{
			name:     "EmptySlicesBIsNil",
			a:        make([]string, 0),
			expected: true,
		},
		{
			name:     "EmptySlicesBothAreNil",
			expected: true,
		},
		{
			name:     "Equal",
			a:        []string{"0", "1", "2"},
			b:        []string{"0", "1", "2"},
			expected: true,
		},
		{
			name: "NotEqualFirstDiffers",
			a:    []string{"0", "1", "2"},
			b:    []string{"42", "1", "2"},
		},
		{
			name: "NotEqualMiddleDiffers",
			a:    []string{"0", "42", "2"},
			b:    []string{"0", "1", "2"},
		},
		{
			name: "NotEqualLastDiffers",
			a:    []string{"0", "1", "2"},
			b:    []string{"0", "1", "42"},
		},
		{
			name: "NotEqualUnsorted",
			a:    []string{"0", "1", "2"},
			b:    []string{"3", "2", "1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, EqualStrings(test.a, test.b))
		})
	}
}
