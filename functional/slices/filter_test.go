package slices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	type test struct {
		name     string
		s        []int
		p        []func(e int) bool
		expected []int
	}

	tests := []*test{
		{
			name:     "NoPredicates",
			s:        []int{0, 2},
			expected: []int{0, 2},
		},
		{
			name:     "OnePredicate",
			s:        []int{0, 2},
			p:        []func(e int) bool{func(e int) bool { return e > 0 }},
			expected: []int{2},
		},
		{
			name: "TwoPredicates",
			s:    []int{0, 2, 4},
			p: []func(e int) bool{
				func(e int) bool { return e > 0 },
				func(e int) bool { return e != 2 },
			},
			expected: []int{4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, Filter(test.s, test.p...))
		})
	}
}

// Not actually testing functionality here, just checking that the type assertion is not "comparable".
func TestFilterNotComparable(t *testing.T) {
	require.Empty(t, Filter([]interface{}{}))
}
