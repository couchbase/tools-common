package cbrest

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testServices = &Services{
		Management:        8091,
		ManagementSSL:     18091,
		KV:                11210,
		KVSSL:             11207,
		CAPI:              8092,
		CAPISSL:           18092,
		CBAS:              8095,
		CBASSSL:           18095,
		Eventing:          8096,
		EventingSSL:       18096,
		FullText:          8094,
		FullTextSSL:       18094,
		SecondaryIndex:    9102,
		SecondaryIndexSSL: 19102,
		N1QL:              8093,
		N1QLSSL:           18093,
		Backup:            7100,
		BackupSSL:         17100,
	}

	testAltServices = &Services{
		Management:        8092,
		ManagementSSL:     18092,
		KV:                11211,
		KVSSL:             11208,
		CAPI:              8093,
		CAPISSL:           18093,
		CBAS:              8096,
		CBASSSL:           18096,
		Eventing:          8097,
		EventingSSL:       18097,
		FullText:          8095,
		FullTextSSL:       18095,
		SecondaryIndex:    9103,
		SecondaryIndexSSL: 19103,
		N1QL:              8094,
		N1QLSSL:           18094,
		Backup:            7101,
		BackupSSL:         17101,
	}
)

func TestNodesUnmarshal(t *testing.T) {
	data := `{"rev":41,"nodesExt":[{"services":{"mgmt":8091,"mgmtSSL":18091,"kv":11210,"kvSSL":11207,` +
		`"capi":8092,"capiSSL":18092,"projector":9999},"thisNode":true,"hostname":"172.20.1.1",` +
		`"alternateAddresses":{"external": {"hostname":"172.20.1.5","ports":{"mgmt":8092,"mgmtSSL":18092}}}},` +
		`{"services":{"mgmt":8091,"mgmtSSL":18091,"kv":11210,"kvSSL":11207,"capi":8092,"capiSSL":18092,` +
		`"projector":9999},"hostname":"172.20.1.2"},{"services":{"mgmt":8091,"mgmtSSL":18091,` +
		`"kv":11210,"kvSSL":11207,"capi":8092,"capiSSL":18092,"projector":9999},"hostname":"172.20.1.3"},` +
		`{"services":{"mgmt":8091,"mgmtSSL":18091,"cbas":8095,"cbasSSL":18095,"eventingAdminPort":8096,` +
		`"eventingDebug":9140,"eventingSSL":18096,"fts":8094,"ftsSSL":18094,"ftsGRPC":9130,` +
		`"ftsGRPCSSL":19130,"indexAdmin":9100,"indexScan":9101,"indexHttp":9102,"indexStreamInit":9103,` +
		`"indexStreamCatchup":9104,"indexStreamMaint":9105,"indexHttps":19102,"n1ql":8093,"n1qlSSL":18093},` +
		`"hostname":"172.20.1.4"}],"clusterCapabilitiesVer":[1,0],` +
		`"clusterCapabilities":{"n1ql":["enhancedPreparedStatements"]}}`

	var nodes Nodes
	err := json.Unmarshal([]byte(data), &nodes)
	require.NoError(t, err)

	expected := Nodes{
		{
			Hostname: "172.20.1.1",
			Services: &Services{
				Management:    8091,
				ManagementSSL: 18091,
				KV:            11210,
				KVSSL:         11207,
				CAPI:          8092,
				CAPISSL:       18092,
			},
			AlternateAddresses: AlternateAddresses{
				External: &External{
					Hostname: "172.20.1.5",
					Services: &Services{
						Management:    8092,
						ManagementSSL: 18092,
					},
				},
			},
		},
		{
			Hostname: "172.20.1.2",
			Services: &Services{
				Management:    8091,
				ManagementSSL: 18091,
				KV:            11210,
				KVSSL:         11207,
				CAPI:          8092,
				CAPISSL:       18092,
			},
		},
		{
			Hostname: "172.20.1.3",
			Services: &Services{
				Management:    8091,
				ManagementSSL: 18091,
				KV:            11210,
				KVSSL:         11207,
				CAPI:          8092,
				CAPISSL:       18092,
			},
		},
		{
			Hostname: "172.20.1.4",
			Services: &Services{
				Management:        8091,
				ManagementSSL:     18091,
				CBAS:              8095,
				CBASSSL:           18095,
				Eventing:          8096,
				EventingSSL:       18096,
				FullText:          8094,
				FullTextSSL:       18094,
				SecondaryIndex:    9102,
				SecondaryIndexSSL: 19102,
				N1QL:              8093,
				N1QLSSL:           18093,
			},
		},
	}

	require.Equal(t, expected, nodes)
}

func TestNodesCopy(t *testing.T) {
	expected := Nodes{
		{
			Hostname: "172.20.1.1",
			Services: &Services{Management: 8091},
			AlternateAddresses: AlternateAddresses{
				External: &External{
					Hostname: "172.20.1.2",
					Services: &Services{Management: 8092},
				},
			},
		},
		{
			Hostname: "172.20.1.1",
			Services: &Services{Management: 8091},
			AlternateAddresses: AlternateAddresses{
				External: &External{
					Hostname: "172.20.1.2",
					Services: &Services{Management: 8092},
				},
			},
		},
	}

	actual := expected.Copy()
	require.Equal(t, expected, actual)
	require.NotSame(t, expected, actual)

	for i := 0; i < len(expected); i++ {
		require.NotSame(t, expected[i], actual[i])
	}
}

func TestNodeCopy(t *testing.T) {
	expected := &Node{
		Hostname: "172.20.1.1",
		Services: &Services{Management: 8091},
		AlternateAddresses: AlternateAddresses{
			External: &External{
				Hostname: "172.20.1.2",
				Services: &Services{Management: 8092},
			},
		},
	}

	actual := expected.Copy()

	require.Equal(t, expected, actual)
	require.NotSame(t, expected, actual)
}

func TestNodeGetHostname(t *testing.T) {
	hostnameNode := &Node{
		Hostname: "hostname",
		Services: &Services{
			Management:    8091,
			ManagementSSL: 18091,
			KV:            11210,
			KVSSL:         11207,
		},
		AlternateAddresses: AlternateAddresses{
			External: &External{
				Hostname: "alternative_hostname",
				Services: &Services{
					Management:    8092,
					ManagementSSL: 18092,
				},
			},
		},
	}

	ipv4Node := &Node{
		Hostname: "172.20.1.1",
		Services: &Services{
			Management:    8091,
			ManagementSSL: 18091,
			KV:            11210,
			KVSSL:         11207,
		},
		AlternateAddresses: AlternateAddresses{
			External: &External{
				Hostname: "172.20.1.5",
				Services: &Services{
					Management:    8092,
					ManagementSSL: 18092,
				},
			},
		},
	}

	ipv6Node := &Node{
		Hostname: "[2001:4860:4860::8888]",
		Services: &Services{
			Management:    8091,
			ManagementSSL: 18091,
			KV:            11210,
			KVSSL:         11207,
		},
		AlternateAddresses: AlternateAddresses{
			External: &External{
				Hostname: "[2001:4860:4860::9999]",
				Services: &Services{
					Management:    8092,
					ManagementSSL: 18092,
				},
			},
		},
	}

	type test struct {
		service    Service
		node       *Node
		useSSL     bool
		useAltAddr bool
		expected   string
	}

	tests := []*test{
		{
			service:  ServiceManagement,
			node:     hostnameNode,
			expected: "http://hostname:8091",
		},
		{
			service:  ServiceManagement,
			node:     hostnameNode,
			useSSL:   true,
			expected: "https://hostname:18091",
		},
		{
			service:    ServiceManagement,
			node:       hostnameNode,
			useAltAddr: true,
			expected:   "http://alternative_hostname:8092",
		},
		{
			service:    ServiceManagement,
			node:       hostnameNode,
			useSSL:     true,
			useAltAddr: true,
			expected:   "https://alternative_hostname:18092",
		},
		{
			service:  ServiceData,
			node:     hostnameNode,
			expected: "http://hostname:11210",
		},
		{
			service:  ServiceData,
			node:     hostnameNode,
			useSSL:   true,
			expected: "https://hostname:11207",
		},
		{
			service:    ServiceData,
			node:       hostnameNode,
			useAltAddr: true,
		},
		{
			service:  ServiceManagement,
			node:     ipv4Node,
			expected: "http://172.20.1.1:8091",
		},
		{
			service:  ServiceManagement,
			node:     ipv4Node,
			useSSL:   true,
			expected: "https://172.20.1.1:18091",
		},
		{
			service:    ServiceManagement,
			node:       ipv4Node,
			useAltAddr: true,
			expected:   "http://172.20.1.5:8092",
		},
		{
			service:    ServiceManagement,
			node:       ipv4Node,
			useSSL:     true,
			useAltAddr: true,
			expected:   "https://172.20.1.5:18092",
		},
		{
			service:  ServiceData,
			node:     ipv4Node,
			expected: "http://172.20.1.1:11210",
		},
		{
			service:  ServiceData,
			node:     ipv4Node,
			useSSL:   true,
			expected: "https://172.20.1.1:11207",
		},
		{
			service:    ServiceData,
			node:       ipv4Node,
			useAltAddr: true,
		},
		{
			service:  ServiceManagement,
			node:     ipv6Node,
			expected: "http://[2001:4860:4860::8888]:8091",
		},
		{
			service:  ServiceManagement,
			node:     ipv6Node,
			useSSL:   true,
			expected: "https://[2001:4860:4860::8888]:18091",
		},
		{
			service:    ServiceManagement,
			node:       ipv6Node,
			useAltAddr: true,
			expected:   "http://[2001:4860:4860::9999]:8092",
		},
		{
			service:    ServiceManagement,
			node:       ipv6Node,
			useSSL:     true,
			useAltAddr: true,
			expected:   "https://[2001:4860:4860::9999]:18092",
		},
		{
			service:  ServiceData,
			node:     ipv6Node,
			expected: "http://[2001:4860:4860::8888]:11210",
		},
		{
			service:  ServiceData,
			node:     ipv6Node,
			useSSL:   true,
			expected: "https://[2001:4860:4860::8888]:11207",
		},
		{
			service:    ServiceData,
			node:       ipv6Node,
			useAltAddr: true,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf(`{"service":"%s","use_ssl":%t,"use_alt_addr":%t}`, test.service, test.useSSL, test.useAltAddr)
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, test.node.GetHostname(test.service, test.useSSL, test.useAltAddr))
		})
	}

	altAddressNode := &Node{
		Hostname: "hostname",
		Services: &Services{},
		AlternateAddresses: AlternateAddresses{
			External: &External{
				Services: &Services{},
			},
		},
	}

	require.Zero(
		t,
		altAddressNode.GetHostname(ServiceData, false, true),
		"Alternate hostname is not populated, expected to get an empty string",
	)

	altAddressNode.AlternateAddresses.External = nil

	require.Zero(
		t,
		altAddressNode.GetHostname(ServiceData, false, true),
		"Alternate hostname is not populated, expected to get an empty string",
	)
}

func TestServicesGetPort(t *testing.T) {
	type test struct {
		service    Service
		useSSL     bool
		useAltAddr bool
		expected   uint16
	}

	tests := []*test{
		{
			service:  ServiceManagement,
			expected: 8091,
		},
		{
			service:  ServiceManagement,
			useSSL:   true,
			expected: 18091,
		},
		{
			service:  ServiceAnalytics,
			expected: 8095,
		},
		{
			service:  ServiceAnalytics,
			useSSL:   true,
			expected: 18095,
		},
		{
			service:  ServiceData,
			expected: 11210,
		},
		{
			service:  ServiceData,
			useSSL:   true,
			expected: 11207,
		},
		{
			service:  ServiceEventing,
			expected: 8096,
		},
		{
			service:  ServiceEventing,
			useSSL:   true,
			expected: 18096,
		},
		{
			service:  ServiceGSI,
			expected: 9102,
		},
		{
			service:  ServiceGSI,
			useSSL:   true,
			expected: 19102,
		},
		{
			service:  ServiceQuery,
			expected: 8093,
		},
		{
			service:  ServiceQuery,
			useSSL:   true,
			expected: 18093,
		},
		{
			service:  ServiceSearch,
			expected: 8094,
		},
		{
			service:  ServiceSearch,
			useSSL:   true,
			expected: 18094,
		},
		{
			service:  ServiceViews,
			expected: 8091,
		},
		{
			service:  ServiceViews,
			useSSL:   true,
			expected: 18091,
		},
		{
			service:    ServiceViews,
			useAltAddr: true,
			expected:   8092,
		},
		{
			service:    ServiceViews,
			useSSL:     true,
			useAltAddr: true,
			expected:   18092,
		},
		{
			service:  ServiceBackup,
			expected: 7100,
		},
		{
			service:  ServiceBackup,
			useSSL:   true,
			expected: 17100,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf(`{"service":"%s","use_ssl":%t,"use_alt_addr":%t}`, test.service, test.useSSL, test.useAltAddr)
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, testServices.GetPort(test.service, test.useSSL, test.useAltAddr))
		})
	}
}

// The test node isn't running the Data Service, therefore, should not be sent any REST requests for 'Views'; we should
// ensure that we get zero value ports, see MB-42446 for more information.
func TestServicesGetPortViewsRequireDataService(t *testing.T) {
	services := &Services{
		Management:    8091,
		ManagementSSL: 18091,
		CAPI:          8092,
		CAPISSL:       18092,
	}

	require.Zero(t, services.GetPort(ServiceViews, false, false))
	require.Zero(t, services.GetPort(ServiceViews, true, false))
}
