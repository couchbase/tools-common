package netutil

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTemporaryFailure(t *testing.T) {
	type test struct {
		name     string
		input    int
		expected bool
	}

	tests := []*test{
		{
			name:     "503",
			input:    http.StatusServiceUnavailable,
			expected: true,
		},
		{
			name:  "404",
			input: http.StatusNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, IsTemporaryFailure(test.input))
		})
	}
}
