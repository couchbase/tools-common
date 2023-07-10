package objval

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderString(t *testing.T) {
	type test struct {
		name     string
		provider Provider
		expected string
	}

	tests := []*test{
		{
			name:     "none",
			provider: ProviderNone,
		},
		{
			name:     "AWS",
			provider: ProviderAWS,
			expected: "AWS",
		},
		{
			name:     "GCP",
			provider: ProviderGCP,
			expected: "GCP",
		},
		{
			name:     "Azure",
			provider: ProviderAzure,
			expected: "Azure",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.provider.String())
		})
	}
}
