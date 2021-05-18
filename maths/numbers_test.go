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
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestMaxInt64(t *testing.T) {
	type testCase struct {
		name     string
		a        int64
		b        int64
		expected int64
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
			actual := MaxInt64(tc.a, tc.b)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestMaxUint32(t *testing.T) {
	type testCase struct {
		name     string
		a        uint32
		b        uint32
		expected uint32
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
			a:        9e8,
			b:        9e8,
			expected: 9e8,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := MaxUint32(tc.a, tc.b)
			require.Equal(t, tc.expected, actual)
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
			require.Equal(t, tc.expected, actual)
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
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestMinInt64(t *testing.T) {
	type testCase struct {
		name     string
		a        int64
		b        int64
		expected int64
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
			actual := MinInt64(tc.a, tc.b)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestMinUint32(t *testing.T) {
	type testCase struct {
		name     string
		a        uint32
		b        uint32
		expected uint32
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
			a:        9e8,
			b:        9e8,
			expected: 9e8,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := MinUint32(tc.a, tc.b)
			require.Equal(t, tc.expected, actual)
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
			require.Equal(t, tc.expected, actual)
		})
	}
}
