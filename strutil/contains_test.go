package strutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContains(t *testing.T) {
	type test struct {
		name     string
		a        string
		b        []string
		expected bool
	}

	tests := []*test{
		{
			name: "NotContains",
			a:    "this string contains",
			b:    []string{"not"},
		},
		{
			name:     "Contains",
			a:        "this string contains",
			b:        []string{"string"},
			expected: true,
		},
		{
			name:     "ContainsSomeNotMatch",
			a:        "this string contains",
			b:        []string{"not", "string"},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, Contains(test.a, test.b...))
		})
	}
}
