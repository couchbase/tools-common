package rest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEndpointFormat(t *testing.T) {
	type test struct {
		name     string
		input    Endpoint
		args     []string
		expected Endpoint
	}

	tests := []*test{
		{
			name:     "EndpointNoEscape",
			input:    "/pools",
			expected: "/pools",
		},
		{
			name:     "EndpointWithEscape",
			input:    "/pools/default/buckets/%s",
			args:     []string{"b/uc%ket"},
			expected: "/pools/default/buckets/b%2Fuc%25ket",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.input.Format(test.args...))
		})
	}
}
