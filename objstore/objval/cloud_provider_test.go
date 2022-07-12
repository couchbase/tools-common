package objval

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloudProviderString(t *testing.T) {
	type test struct {
		input    CloudProvider
		expected string
	}

	tests := []*test{
		{
			input: CloudProviderNone,
		},
		{
			input:    CloudProviderAWS,
			expected: "AWS",
		},
		{
			input:    CloudProviderAzure,
			expected: "Azure",
		},
		{
			input:    CloudProviderGCP,
			expected: "GCP",
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.input.String())
	}
}
