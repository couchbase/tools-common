package slices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSum(t *testing.T) {
	type test struct {
		name     string
		s        []float64
		expected float64
	}

	tests := []*test{
		{
			name: "NilSlice",
		},
		{
			name: "EmptySlice",
			s:    make([]float64, 0),
		},
		{
			name:     "SumSingleElement",
			s:        []float64{42.0},
			expected: 42.0,
		},
		{
			name:     "SumMultiElement",
			s:        []float64{128.0, 42.0, 2.56},
			expected: 172.56,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Sum(test.s)
			require.Equal(t, test.expected, actual)
		})
	}
}
