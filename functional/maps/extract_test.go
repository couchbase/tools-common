package maps

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	type test struct {
		name     string
		m        map[int]int
		p        []func(k, v int) bool
		expected []int
	}

	tests := []*test{
		{
			name:     "NoPredicates",
			m:        map[int]int{0: 1, 2: 3},
			expected: []int{0, 2},
		},
		{
			name:     "OnePredicate",
			m:        map[int]int{0: 1, 2: 3},
			p:        []func(k, v int) bool{func(k, v int) bool { return k > 0 && v > 0 }},
			expected: []int{2},
		},
		{
			name: "TwoPredicates",
			m:    map[int]int{0: 1, 2: 3, 4: 4},
			p: []func(k, v int) bool{
				func(k, v int) bool { return k > 0 && v > 0 },
				func(k, v int) bool { return k != v },
			},
			expected: []int{2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Keys(test.m, test.p...)

			sort.Ints(actual)
			sort.Ints(test.expected)

			require.Equal(t, test.expected, actual)
		})
	}
}

func TestValues(t *testing.T) {
	type test struct {
		name     string
		m        map[int]int
		p        []func(k, v int) bool
		expected []int
	}

	tests := []*test{
		{
			name:     "NoPredicates",
			m:        map[int]int{0: 1, 2: 3},
			expected: []int{1, 3},
		},
		{
			name:     "OnePredicate",
			m:        map[int]int{0: 1, 2: 3},
			p:        []func(k, v int) bool{func(k, v int) bool { return k > 0 && v > 0 }},
			expected: []int{3},
		},
		{
			name: "TwoPredicates",
			m:    map[int]int{0: 1, 2: 3, 4: 4},
			p: []func(k, v int) bool{
				func(k, v int) bool { return k > 0 && v > 0 },
				func(k, v int) bool { return k != v },
			},
			expected: []int{3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Values(test.m, test.p...)

			sort.Ints(actual)
			sort.Ints(test.expected)

			require.Equal(t, test.expected, actual)
		})
	}
}
