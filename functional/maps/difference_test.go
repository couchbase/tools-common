package maps

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifference(t *testing.T) {
	type test struct {
		name           string
		a, b, expected map[string]int
	}

	tests := []test{
		{
			name:     "Nil",
			expected: make(map[string]int),
		},
		{
			name:     "ANil",
			b:        map[string]int{"a": 1, "b": 2, "c": 3},
			expected: make(map[string]int),
		},
		{
			name:     "BNil",
			a:        map[string]int{"a": 1, "b": 2, "c": 3},
			expected: map[string]int{"a": 1, "b": 2, "c": 3},
		},
		{
			name:     "Empty",
			a:        make(map[string]int),
			b:        make(map[string]int),
			expected: make(map[string]int),
		},
		{
			name:     "Equal",
			a:        map[string]int{"a": 1, "b": 2, "c": 3},
			b:        map[string]int{"a": 1, "b": 2, "c": 3},
			expected: make(map[string]int),
		},
		{
			name:     "SingleDifference",
			a:        map[string]int{"a": 1, "d": 4, "c": 3},
			b:        map[string]int{"a": 1, "b": 2, "c": 3},
			expected: map[string]int{"d": 4},
		},
		{
			name:     "MultiDifference",
			a:        map[string]int{"a": 1, "d": 4, "e": 5},
			b:        map[string]int{"a": 1, "b": 2, "c": 3},
			expected: map[string]int{"d": 4, "e": 5},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, Difference(test.a, test.b))
		})
	}
}
