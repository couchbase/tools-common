package slice

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnionString(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected []string
	}{
		{
			name:     "disjoint",
			a:        []string{"0", "1", "2", "3"},
			b:        []string{"a", "b"},
			expected: []string{},
		},
		{
			name:     "equal",
			a:        []string{"0", "1", "2", "3"},
			b:        []string{"2", "1", "0", "3"},
			expected: []string{"0", "1", "2", "3"},
		},
		{
			name:     "partial_match",
			a:        []string{"alpha", "tango", "papa", "romeo"},
			b:        []string{"bravo", "charlie", "echo", "tango", "quebec", "romeo", "zulu"},
			expected: []string{"romeo", "tango"},
		},
		{
			name:     "nil-input",
			a:        []string{"alpha"},
			expected: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := Union(tc.a, tc.b)
			sort.Strings(out)
			require.Equal(t, tc.expected, out)
		})
	}
}
