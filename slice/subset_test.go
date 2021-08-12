package slice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubsetStrings(t *testing.T) {
	type test struct {
		name     string
		a, b     []string
		expected bool
	}

	tests := []*test{
		{
			name:     "BothEmpty",
			expected: true,
		},
		{
			name:     "AEmpty",
			b:        []string{"a", "b", "c"},
			expected: true,
		},
		{
			name: "BEmpty",
			a:    []string{"a", "b", "c"},
		},
		{
			name:     "IsSubsetFirst",
			a:        []string{"a"},
			b:        []string{"a", "b", "c"},
			expected: true,
		},
		{
			name:     "IsSubsetMiddle",
			a:        []string{"b"},
			b:        []string{"a", "b", "c"},
			expected: true,
		},
		{
			name:     "IsSubsetLast",
			a:        []string{"c"},
			b:        []string{"a", "b", "c"},
			expected: true,
		},
		{
			name: "NotSubset",
			a:    []string{"d"},
			b:    []string{"a", "b", "c"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, SubsetStrings(test.a, test.b))
		})
	}
}
