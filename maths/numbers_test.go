package maths

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxUint64(t *testing.T) {
	type testCase struct {
		name     string
		a        uint64
		b        uint64
		expected uint64
	}

	cases := []testCase{
		{
			name:     "normal",
			a:        550,
			b:        8e6,
			expected: 8e6,
		},
		{
			name:     "zero-value",
			a:        20,
			b:        0,
			expected: 20,
		},
		{
			name:     "same",
			a:        9e15,
			b:        9e15,
			expected: 9e15,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := MaxUint64(tc.a, tc.b)
			require.Equal(t, actual, tc.expected)
		})
	}
}

func TestMax(t *testing.T) {
	type testCase struct {
		name     string
		a        int
		b        int
		expected int
	}

	cases := []testCase{
		{
			name:     "normal",
			a:        550,
			b:        8e6,
			expected: 8e6,
		},
		{
			name:     "zero-value",
			a:        20,
			b:        0,
			expected: 20,
		},
		{
			name:     "negative",
			a:        -9e10,
			b:        9e5,
			expected: 9e5,
		},
		{
			name:     "same",
			a:        9e5,
			b:        9e5,
			expected: 9e5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := Max(tc.a, tc.b)
			require.Equal(t, actual, tc.expected)
		})
	}
}

func TestMinUint64(t *testing.T) {
	type testCase struct {
		name     string
		a        uint64
		b        uint64
		expected uint64
	}

	cases := []testCase{
		{
			name:     "normal",
			a:        550,
			b:        8e6,
			expected: 550,
		},
		{
			name:     "zero-value",
			a:        20,
			b:        0,
			expected: 0,
		},
		{
			name:     "same",
			a:        9e15,
			b:        9e15,
			expected: 9e15,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := MinUint64(tc.a, tc.b)
			require.Equal(t, actual, tc.expected)
		})
	}
}

func TestMin(t *testing.T) {
	type testCase struct {
		name     string
		a        int
		b        int
		expected int
	}

	cases := []testCase{
		{
			name:     "positive",
			a:        550,
			b:        8e6,
			expected: 550,
		},
		{
			name:     "zero-value",
			a:        20,
			b:        0,
			expected: 0,
		},
		{
			name:     "negative",
			a:        -20,
			b:        0,
			expected: -20,
		},
		{
			name:     "same",
			a:        -20,
			b:        -20,
			expected: -20,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := Min(tc.a, tc.b)
			require.Equal(t, actual, tc.expected)
		})
	}
}
