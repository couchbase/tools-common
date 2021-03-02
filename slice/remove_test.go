package slice

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveStringAt(t *testing.T) {
	tests := []struct {
		name          string
		container     []string
		index         int
		expected      []string
		expectedError bool
	}{
		{
			name:          "empty",
			expectedError: true,
		},
		{
			name:          "minus_one_index",
			container:     []string{"a", "b", "c"},
			index:         -1,
			expectedError: true,
		},
		{
			name:          "index-to-large",
			container:     []string{"a", "b", "c"},
			index:         100,
			expectedError: true,
		},
		{
			name:      "start",
			container: []string{"a", "b", "c"},
			index:     0,
			expected:  []string{"b", "c"},
		},
		{
			name:      "middle",
			container: []string{"a", "b", "c"},
			index:     1,
			expected:  []string{"a", "c"},
		},
		{
			name:      "last",
			container: []string{"a", "b", "c"},
			index:     2,
			expected:  []string{"a", "b"},
		},
		{
			name:      "only",
			container: []string{"a"},
			index:     0,
			expected:  []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := RemoveStringAt(tc.container, tc.index)
			require.Equal(t, tc.expectedError, err != nil, "error not what was expected %v", err)
			if err != nil {
				return
			}

			sort.Strings(out)
			require.Equal(t, tc.expected, out)
		})
	}
}
