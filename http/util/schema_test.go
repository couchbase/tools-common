package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrimSchema(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected string
	}

	tests := []*test{
		{
			name:     "NoSchemaShouldIgnore",
			input:    "hostname:8091",
			expected: "hostname:8091",
		},
		{
			name:     "HTTP",
			input:    "http://hostname:8091",
			expected: "hostname:8091",
		},
		{
			name:     "HTTPS",
			input:    "https://hostname:8091",
			expected: "hostname:8091",
		},
		{
			name:     "Couchbase",
			input:    "couchbase://hostname:8091",
			expected: "hostname:8091",
		},
		{
			name:     "Couchbases",
			input:    "couchbases://hostname:8091",
			expected: "hostname:8091",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, TrimSchema(test.input))
		})
	}
}

func TestToCouchbaseSchema(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected string
	}

	tests := []*test{
		{
			name:     "NoSchemaShouldIgnore",
			input:    "hostname:8091",
			expected: "hostname:8091",
		},
		{
			name:     "HTTP",
			input:    "http://hostname:8091",
			expected: "couchbase://hostname:8091",
		},
		{
			name:     "HTTPS",
			input:    "https://hostname:8091",
			expected: "couchbases://hostname:8091",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, ToCouchbaseSchema(test.input))
		})
	}
}
