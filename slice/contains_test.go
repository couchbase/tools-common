package slice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsString(t *testing.T) {
	type test struct {
		name     string
		s        []string
		e        string
		expected bool
	}

	tests := []*test{
		{
			name: "NilSlice",
		},
		{
			name: "EmptySlice",
			s:    make([]string, 0),
		},
		{
			name:     "ContainsSingleElement",
			s:        []string{"element"},
			e:        "element",
			expected: true,
		},
		{
			name:     "ContainsMultiElement",
			s:        []string{"this is not the string you're looking for", "element"},
			e:        "element",
			expected: true,
		},
		{
			name: "NotContainsSingleElement",
			s:    []string{"this is not the string you're looking for"},
			e:    "element",
		},
		{
			name: "NotContainsMultiElement",
			s:    []string{"this is not the string you're looking for", "eL3meNt"},
			e:    "element",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := ContainsString(test.s, test.e)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestContainsInt(t *testing.T) {
	type test struct {
		name     string
		s        []int
		e        int
		expected bool
	}

	tests := []*test{
		{
			name: "NilSlice",
		},
		{
			name: "EmptySlice",
			s:    make([]int, 0),
		},
		{
			name:     "ContainsSingleElement",
			s:        []int{42},
			e:        42,
			expected: true,
		},
		{
			name:     "ContainsMultiElement",
			s:        []int{128, 42},
			e:        42,
			expected: true,
		},
		{
			name: "NotContainsSingleElement",
			s:    []int{128},
			e:    42,
		},
		{
			name: "NotContainsMultiElement",
			s:    []int{128, 256},
			e:    42,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := ContainsInt(test.s, test.e)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestContainsUint16(t *testing.T) {
	type test struct {
		name     string
		s        []uint16
		e        uint16
		expected bool
	}

	tests := []*test{
		{
			name: "NilSlice",
		},
		{
			name: "EmptySlice",
			s:    make([]uint16, 0),
		},
		{
			name:     "ContainsSingleElement",
			s:        []uint16{42},
			e:        42,
			expected: true,
		},
		{
			name:     "ContainsMultiElement",
			s:        []uint16{128, 42},
			e:        42,
			expected: true,
		},
		{
			name: "NotContainsSingleElement",
			s:    []uint16{128},
			e:    42,
		},
		{
			name: "NotContainsMultiElement",
			s:    []uint16{128, 256},
			e:    42,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := ContainsUint16(test.s, test.e)
			require.Equal(t, test.expected, actual)
		})
	}
}
