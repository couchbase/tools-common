package connstr

import (
	"fmt"
	"math"
	"net"
	"testing"

	mockdns "github.com/foxcpp/go-mockdns"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	type test struct {
		name          string
		input         string
		expected      *ConnectionString
		expectedError error
	}

	tests := []*test{
		{
			name:          "EmptyInput",
			expectedError: ErrNoAddressesParsed,
		},
		{
			name:          "BadScheme",
			input:         "s3://bucket",
			expectedError: ErrBadScheme,
		},
		{
			name:          "BadPort",
			input:         fmt.Sprintf("localhost:%d", math.MaxUint16+1),
			expectedError: ErrBadPort,
		},
		{
			name:     "ValidHTTPHostNoPort",
			input:    "http://localhost",
			expected: &ConnectionString{Scheme: "http", Addresses: []Address{{Host: "localhost"}}},
		},
		{
			name:     "ValidHTTPHostWithPort",
			input:    "http://localhost:12345",
			expected: &ConnectionString{Scheme: "http", Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:     "ValidHTTPSHostNoPort",
			input:    "https://localhost",
			expected: &ConnectionString{Scheme: "https", Addresses: []Address{{Host: "localhost"}}},
		},
		{
			name:     "ValidHTTPSHostNoPort",
			input:    "https://localhost:12345",
			expected: &ConnectionString{Scheme: "https", Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:     "ValidCouchbaseHostNoPort",
			input:    "couchbase://localhost",
			expected: &ConnectionString{Scheme: "couchbase", Addresses: []Address{{Host: "localhost"}}},
		},
		{
			name:     "ValidCouchbaseHostWithPort",
			input:    "couchbase://localhost:12345",
			expected: &ConnectionString{Scheme: "couchbase", Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:     "ValidCouchbasesHostNoPort",
			input:    "couchbases://localhost",
			expected: &ConnectionString{Scheme: "couchbases", Addresses: []Address{{Host: "localhost"}}},
		},
		{
			name:     "ValidCouchbasesHostWithPort",
			input:    "couchbases://localhost:12345",
			expected: &ConnectionString{Scheme: "couchbases", Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:     "ValidNoSchemeNoPort",
			input:    "localhost",
			expected: &ConnectionString{Addresses: []Address{{Host: "localhost"}}},
		},
		{
			name:     "ValidNoSchemeWithPort",
			input:    "localhost:12345",
			expected: &ConnectionString{Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:  "ValidMultipleHostsNoSchemeNoPort",
			input: "host1,host2,host3,host4",
			expected: &ConnectionString{
				Addresses: []Address{{Host: "host1"}, {Host: "host2"}, {Host: "host3"}, {Host: "host4"}},
			},
		},
		{
			name:  "ValidMultipleHostsNoSchemeWithPort",
			input: "host1:1,host2:2,host3:3,host4:4",
			expected: &ConnectionString{Addresses: []Address{
				{Host: "host1", Port: 1},
				{Host: "host2", Port: 2},
				{Host: "host3", Port: 3},
				{Host: "host4", Port: 4},
			}},
		},
		{
			name:  "ValidMultipleHostsNoPort",
			input: "https://host1,host2,host3,host4",
			expected: &ConnectionString{
				Scheme:    "https",
				Addresses: []Address{{Host: "host1"}, {Host: "host2"}, {Host: "host3"}, {Host: "host4"}},
			},
		},
		{
			name:  "ValidMultipleHostsSchemeWithPort",
			input: "couchbases://host1:1,host2:2,host3:3,host4:4",
			expected: &ConnectionString{
				Scheme: "couchbases",
				Addresses: []Address{
					{Host: "host1", Port: 1},
					{Host: "host2", Port: 2},
					{Host: "host3", Port: 3},
					{Host: "host4", Port: 4},
				},
			},
		},
		{
			name:     "ValidIPV6WithSchemeNoPort",
			input:    "https://[2001:4860:4860::8888]",
			expected: &ConnectionString{Scheme: "https", Addresses: []Address{{Host: "[2001:4860:4860::8888]"}}},
		},
		{
			name:     "ValidIPV6NoSchemeNoPort",
			input:    "[2001:4860:4860::8888]",
			expected: &ConnectionString{Addresses: []Address{{Host: "[2001:4860:4860::8888]"}}},
		},
		{
			name:     "ValidIPV6NoSchemeWithPort",
			input:    "[2001:4860:4860::8888]:12345",
			expected: &ConnectionString{Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}}},
		},
		{
			name:  "ValidIPV6WithSchemeAndPort",
			input: "couchbase://[2001:4860:4860::8888]:12345",
			expected: &ConnectionString{
				Scheme:    "couchbase",
				Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}},
			},
		},
		{
			name:  "ValidWithQueryParam",
			input: "couchbase://[2001:4860:4860::8888]:12345?network=external",
			expected: &ConnectionString{
				Scheme:    "couchbase",
				Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}},
				Params:    map[string][]string{"network": {"external"}},
			},
		},
		{
			name:  "ValidWithMultiValueQueryParams",
			input: "couchbase://[2001:4860:4860::8888]:12345?network=internal&network=external",
			expected: &ConnectionString{
				Scheme:    "couchbase",
				Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}},
				Params:    map[string][]string{"network": {"internal", "external"}},
			},
		},
		{
			name:  "ValidWithMultipleQueryParams",
			input: "couchbase://[2001:4860:4860::8888]:12345?a=b&b=a",
			expected: &ConnectionString{
				Scheme:    "couchbase",
				Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}},
				Params:    map[string][]string{"a": {"b"}, "b": {"a"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := Parse(test.input)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}

			require.Nil(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestConnectionStringResolve(t *testing.T) {
	type test struct {
		name          string
		input         string
		expected      *ResolvedConnectionString
		expectedError error
		zones         map[string]mockdns.Zone
	}

	tests := []*test{
		{
			name:     "ValidHTTPHostNoPort",
			input:    "http://localhost",
			expected: &ResolvedConnectionString{Addresses: []Address{{Host: "localhost", Port: DefaultHTTPPort}}},
		},
		{
			name:     "ValidHTTPHostWithPort",
			input:    "http://localhost:12345",
			expected: &ResolvedConnectionString{Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:  "ValidHTTPSHostNoPort",
			input: "https://localhost",
			expected: &ResolvedConnectionString{
				UseSSL:    true,
				Addresses: []Address{{Host: "localhost", Port: DefaultHTTPSPort}},
			},
		},
		{
			name:  "ValidHTTPSHostNoPort",
			input: "https://localhost:12345",
			expected: &ResolvedConnectionString{
				UseSSL:    true,
				Addresses: []Address{{Host: "localhost", Port: 12345}},
			},
		},
		{
			name:     "ValidCouchbaseHostNoPort",
			input:    "couchbase://localhost",
			expected: &ResolvedConnectionString{Addresses: []Address{{Host: "localhost", Port: DefaultHTTPPort}}},
		},
		{
			name:     "ValidCouchbaseHostWithPort",
			input:    "couchbase://localhost:12345",
			expected: &ResolvedConnectionString{Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:  "ValidCouchbasesHostNoPort",
			input: "couchbases://localhost",
			expected: &ResolvedConnectionString{
				UseSSL:    true,
				Addresses: []Address{{Host: "localhost", Port: DefaultHTTPSPort}},
			},
		},
		{
			name:  "ValidCouchbasesHostWithPort",
			input: "couchbases://localhost:12345",
			expected: &ResolvedConnectionString{
				UseSSL:    true,
				Addresses: []Address{{Host: "localhost", Port: 12345}},
			},
		},
		{
			name:     "ValidNoSchemeNoPort",
			input:    "localhost",
			expected: &ResolvedConnectionString{Addresses: []Address{{Host: "localhost", Port: DefaultHTTPPort}}},
		},
		{
			name:     "InvalidNoSchemeWithPort",
			input:    "localhost:12345",
			expected: &ResolvedConnectionString{Addresses: []Address{{Host: "localhost", Port: 12345}}},
		},
		{
			name:  "ValidMultipleHostsNoSchemeNoPort",
			input: "host1,host2,host3,host4",
			expected: &ResolvedConnectionString{
				Addresses: []Address{
					{Host: "host1", Port: DefaultHTTPPort},
					{Host: "host2", Port: DefaultHTTPPort},
					{Host: "host3", Port: DefaultHTTPPort},
					{Host: "host4", Port: DefaultHTTPPort},
				},
			},
		},
		{
			name:  "ValidMultipleHostsNoSchemeWithPort",
			input: "host1:1,host2:2,host3:3,host4:4",
			expected: &ResolvedConnectionString{
				Addresses: []Address{
					{Host: "host1", Port: 1},
					{Host: "host2", Port: 2},
					{Host: "host3", Port: 3},
					{Host: "host4", Port: 4},
				},
			},
		},
		{
			name:  "ValidMultipleHostsNoPort",
			input: "https://host1,host2,host3,host4",
			expected: &ResolvedConnectionString{
				UseSSL: true,
				Addresses: []Address{
					{Host: "host1", Port: DefaultHTTPSPort},
					{Host: "host2", Port: DefaultHTTPSPort},
					{Host: "host3", Port: DefaultHTTPSPort},
					{Host: "host4", Port: DefaultHTTPSPort},
				},
			},
		},
		{
			name:  "ValidMultipleHostsSchemeWithPort",
			input: "couchbases://host1:1,host2:2,host3:3,host4:4",
			expected: &ResolvedConnectionString{
				UseSSL: true,
				Addresses: []Address{
					{Host: "host1", Port: 1},
					{Host: "host2", Port: 2},
					{Host: "host3", Port: 3},
					{Host: "host4", Port: 4},
				},
			},
		},
		{
			name:  "ValidIPV6WithSchemeNoPort",
			input: "https://[2001:4860:4860::8888]",
			expected: &ResolvedConnectionString{
				UseSSL: true, Addresses: []Address{
					{Host: "[2001:4860:4860::8888]", Port: DefaultHTTPSPort},
				},
			},
		},
		{
			name:  "ValidIPV6NoSchemeNoPort",
			input: "[2001:4860:4860::8888]",
			expected: &ResolvedConnectionString{
				Addresses: []Address{
					{Host: "[2001:4860:4860::8888]", Port: DefaultHTTPPort},
				},
			},
		},
		{
			name:  "InvalidIPV6NoSchemeWithPort",
			input: "[2001:4860:4860::8888]:12345",
			expected: &ResolvedConnectionString{
				Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}},
			},
		},
		{
			name:  "ValidIPV6WithSchemeAndPort",
			input: "couchbase://[2001:4860:4860::8888]:12345",
			expected: &ResolvedConnectionString{
				Addresses: []Address{{Host: "[2001:4860:4860::8888]", Port: 12345}},
			},
		},
		{
			name:  "ValidSRVNoTLS",
			input: "couchbase://example.com",
			expected: &ResolvedConnectionString{
				Addresses: []Address{{Host: "example.org", Port: DefaultHTTPPort}},
			},
			zones: map[string]mockdns.Zone{
				"_couchbase._tcp.example.com.": {SRV: []net.SRV{{Target: "example.org.", Port: 11210}}},
			},
		},
		{
			name:  "ValidSRVTLS",
			input: "couchbases://example.com",
			expected: &ResolvedConnectionString{
				UseSSL:    true,
				Addresses: []Address{{Host: "example.org", Port: DefaultHTTPSPort}},
			},
			zones: map[string]mockdns.Zone{
				"_couchbases._tcp.example.com.": {SRV: []net.SRV{{Target: "example.org.", Port: 11207}}},
			},
		},
		{
			name:  "ValidSRVMultipleHostsNoTLS",
			input: "couchbase://example.com",
			expected: &ResolvedConnectionString{
				Addresses: []Address{
					{Host: "example1.org", Port: DefaultHTTPPort},
					{Host: "example2.org", Port: DefaultHTTPPort},
				},
			},
			zones: map[string]mockdns.Zone{
				"_couchbase._tcp.example.com.": {SRV: []net.SRV{
					{Target: "example1.org.", Port: 11210},
					{Target: "example2.org.", Port: 11210},
				}},
			},
		},
		{
			name:  "ValidSRVMultipleHostsTLS",
			input: "couchbases://example.com",
			expected: &ResolvedConnectionString{
				UseSSL: true,
				Addresses: []Address{
					{Host: "example1.org", Port: DefaultHTTPSPort},
					{Host: "example2.org", Port: DefaultHTTPSPort},
				},
			},
			zones: map[string]mockdns.Zone{
				"_couchbases._tcp.example.com.": {SRV: []net.SRV{
					{Target: "example1.org.", Port: 11210},
					{Target: "example2.org.", Port: 11210},
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, err := mockdns.NewServer(test.zones, false)
			require.NoError(t, err)
			defer server.Close()

			server.PatchNet(net.DefaultResolver)
			defer mockdns.UnpatchNet(net.DefaultResolver)

			parsed, err := Parse(test.input)
			require.Nil(t, err)

			actual, err := parsed.Resolve()
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}

			require.Nil(t, err)
			require.Equal(t, test.expected, actual)
		})
	}

	t.Run("SchemeModifiedAfterParse", func(t *testing.T) {
		_, err := (&ConnectionString{Scheme: "s3"}).Resolve()
		require.ErrorIs(t, err, ErrBadScheme)
	})

	t.Run("FailedToResolveAnyAddresses", func(t *testing.T) {
		_, err := (&ConnectionString{Scheme: "https", Addresses: []Address{}}).Resolve()
		require.ErrorIs(t, err, ErrNoAddressesResolved)
	})
}
