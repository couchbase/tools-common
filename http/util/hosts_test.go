package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHostsToConnectionString(t *testing.T) {
	type test struct {
		name     string
		input    []string
		expected string
	}

	tests := []*test{
		{
			name:     "SingleHostNoPrefixWithPort",
			input:    []string{"hostname:11210"},
			expected: "hostname:11210",
		},
		{
			name:     "SingleHostWithHTTPSPrefixAndPort",
			input:    []string{"https://hostname:11210"},
			expected: "https://hostname:11210",
		},
		{
			name:     "SingleHostWithCouchbasePrefixAndPort",
			input:    []string{"couchbase://hostname:11210"},
			expected: "couchbase://hostname:11210",
		},
		{
			name:     "MultiHostWithPrefixAndPort",
			input:    []string{"couchbase://hostname1:11210", "couchbase://hostname2:11210"},
			expected: "couchbase://hostname1:11210,hostname2:11210",
		},
		{
			name:     "MultiHostWithPrefixAndPortMixedStyle",
			input:    []string{"couchbase://hostname:11210", "couchbase://[::1]:11210"},
			expected: "couchbase://hostname:11210,[::1]:11210",
		},
		{
			name:     "MultiHostWithHTTPPrefix",
			input:    []string{"http://hostname:11210", "http://[::1]:11210"},
			expected: "http://hostname:11210,[::1]:11210",
		},
		{
			name:     "MultiHostWithHTTPSPrefix",
			input:    []string{"https://hostname:11210", "https://[::1]:11210"},
			expected: "https://hostname:11210,[::1]:11210",
		},
		{
			name:     "MultiHostWithCouchbasePrefix",
			input:    []string{"couchbase://hostname:11210", "couchbase://[::1]:11210"},
			expected: "couchbase://hostname:11210,[::1]:11210",
		},
		{
			name:     "MultiHostWithCouchbasePrefix",
			input:    []string{"couchbases://hostname:11210", "couchbases://[::1]:11210"},
			expected: "couchbases://hostname:11210,[::1]:11210",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, HostsToConnectionString(test.input))
		})
	}
}
