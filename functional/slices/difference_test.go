package slices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifference(t *testing.T) {
	type test struct {
		name           string
		a, b, expected []string
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
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Empty",
			a:        make([]string, 0),
			b:        make([]string, 0),
			expected: make([]string, 0),
		},
		{
			name:     "Equal",
			a:        []string{"a", "b", "c"},
			b:        []string{"a", "b", "c"},
			expected: make([]string, 0),
		},
		{
			name:     "SingleDifference",
			a:        []string{"a", "d", "c"},
			b:        []string{"a", "b", "c"},
			expected: []string{"d"},
		},
		{
			name:     "MultiDifference",
			a:        []string{"a", "d", "e"},
			b:        []string{"a", "b", "c"},
			expected: []string{"d", "e"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, Difference(test.a, test.b))
		})
	}
}
