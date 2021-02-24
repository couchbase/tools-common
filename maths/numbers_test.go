package maths

import "testing"

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
			if out := MaxUint64(tc.a, tc.b); out != tc.expected {
				t.Fatalf("Expected %d got %d", tc.expected, out)
			}
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
			if out := Max(tc.a, tc.b); out != tc.expected {
				t.Fatalf("Expected %d got %d", tc.expected, out)
			}
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
			if out := MinUint64(tc.a, tc.b); out != tc.expected {
				t.Fatalf("Expected %d got %d", tc.expected, out)
			}
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
			if out := Min(tc.a, tc.b); out != tc.expected {
				t.Fatalf("Expected %d got %d", tc.expected, out)
			}
		})
	}
}
