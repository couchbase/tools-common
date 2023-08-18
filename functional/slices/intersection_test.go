package slices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntersection(t *testing.T) {
	type test struct {
		name     string
		a        []string
		b        []string
		expected []string
	}

	tests := []test{
		{
			name:     "Nil",
			expected: make([]string, 0),
		},
		{
			name:     "ANil",
			b:        []string{"a", "b", "c"},
			expected: make([]string, 0),
		},
		{
			name:     "BNil",
			a:        []string{"a", "b", "c"},
			expected: make([]string, 0),
		},
		{
			name:     "Empty",
			a:        make([]string, 0),
			b:        make([]string, 0),
			expected: make([]string, 0),
		},
		{
			name:     "InANotB",
			a:        []string{"a"},
			expected: make([]string, 0),
		},
		{
			name:     "InBNotA",
			b:        []string{"a"},
			expected: make([]string, 0),
		},
		{
			name:     "InBoth",
			a:        []string{"a"},
			b:        []string{"a"},
			expected: []string{"a"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ElementsMatch(t, test.expected, Intersection(test.a, test.b))
		})
	}
}
