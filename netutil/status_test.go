package netutil

import (
	"net/http"
	"strconv"
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
			name:  "404",
			input: http.StatusNotFound,
		},
	}

	// Ensure we're adding an expected number of test cases below
	require.Len(t, TemporaryFailureStatusCodes, 7)

	for status := range TemporaryFailureStatusCodes {
		tests = append(tests, &test{
			name:     strconv.Itoa(status),
			input:    status,
			expected: true,
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, IsTemporaryFailure(test.input))
		})
	}
}
