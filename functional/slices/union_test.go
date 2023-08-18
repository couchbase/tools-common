package slices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnion(t *testing.T) {
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
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "BNil",
			a:        []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Empty",
			a:        make([]string, 0),
			b:        make([]string, 0),
			expected: make([]string, 0),
		},
		{
			name:     "InA",
			a:        []string{"a"},
			expected: []string{"a"},
		},
		{
			name:     "InB",
			b:        []string{"a"},
			expected: []string{"a"},
		},
		{
			name:     "MultipleInA",
			a:        []string{"a", "a"},
			expected: []string{"a"},
		},
		{
			name:     "MultipleInB",
			b:        []string{"a", "a"},
			expected: []string{"a"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ElementsMatch(t, test.expected, Union(test.a, test.b))
		})
	}
}
