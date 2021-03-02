package slice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindString(t *testing.T) {
	tests := []struct {
		name      string
		container []string
		target    string
		expected  int
	}{
		{
			name:     "empty",
			target:   "alpha",
			expected: -1,
		},
		{
			name:      "in",
			container: []string{"alpha", "bravo", "charlie"},
			target:    "charlie",
			expected:  2,
		},
		{
			name:      "not-in",
			container: []string{"alpha", "bravo", "charlie"},
			target:    "delta",
			expected:  -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, FindString(tc.container, tc.target))
		})
	}
}
