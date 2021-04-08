package cbrest

import (
	"testing"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/connstr"

	"github.com/stretchr/testify/require"
)

func TestNewAuthProvider(t *testing.T) {
	actual := NewAuthProvider(
		&connstr.ResolvedConnectionString{},
		&aprov.Static{Username: username, Password: password, UserAgent: userAgent},
	)

	// Don't compare the time attribute from the config manager
	actual.manager.last = nil

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{},
		provider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		manager:  &ClusterConfigManager{maxAge: DefaultCCMaxAge},
	}

	require.Equal(t, expected, actual)
}

func TestAuthProviderGetServiceHost(t *testing.T) {
	type test struct {
		name     string
		provider *AuthProvider
		service  Service
		expected string
	}

	tests := []*test{
		{
			name: "SingleNode",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{{Hostname: "localhost", Services: testServices}},
					},
				},
			},
			service:  ServiceManagement,
			expected: "http://localhost:8091",
		},
		{
			name: "SingleNodeUseSSL",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
					UseSSL:    true,
				},
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{{Hostname: "localhost", Services: testServices}},
					},
				},
			},
			service:  ServiceManagement,
			expected: "https://localhost:18091",
		},
		{
			name: "SingleNodeUseAltAddr",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				useAltAddr: true,
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{{
							Hostname: "localhost",
							Services: testServices,
							AlternateAddresses: AlternateAddresses{
								External: &External{Hostname: "hostname", Services: testAltServices},
							},
						}},
					},
				},
			},
			service:  ServiceManagement,
			expected: "http://hostname:8092",
		},
		{
			name: "SingleNodeUseSSLAndAltAddr",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
					UseSSL:    true,
				},
				useAltAddr: true,
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{{
							Hostname: "localhost",
							Services: testServices,
							AlternateAddresses: AlternateAddresses{
								External: &External{
									Hostname: "hostname",
									Services: testAltServices,
								},
							},
						}},
					},
				},
			},
			service:  ServiceManagement,
			expected: "https://hostname:18092",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.provider.GetServiceHost(test.service)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestAuthProviderGetAllServiceHosts(t *testing.T) {
	kvOnlyService := &Services{
		KV:      11210,
		KVSSL:   11207,
		CAPI:    8092,
		CAPISSL: 18092,
	}

	type test struct {
		name          string
		provider      *AuthProvider
		service       Service
		expected      []string
		expectedError error
	}

	tests := []*test{
		{
			name: "SingleNodeAllServices",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{{Hostname: "localhost", Services: testServices}},
					},
				},
			},
			service:  ServiceManagement,
			expected: []string{"http://localhost:8091"},
		},
		{
			name: "MultiNodeAllServices",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{
							{Hostname: "host1", Services: testServices}, {Hostname: "host2", Services: testServices},
						},
					},
				},
			},
			service:  ServiceManagement,
			expected: []string{"http://host1:8091", "http://host2:8091"},
		},
		{
			name: "MultiNodeMixedServices",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{
							{Hostname: "host1", Services: testServices}, {Hostname: "host2", Services: kvOnlyService},
						},
					},
				},
			},
			service:  ServiceManagement,
			expected: []string{"http://host1:8091"},
		},
		{
			name: "MultiNodeMixedServicesSSL",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
					UseSSL:    true,
				},
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{
							{Hostname: "host1", Services: testServices}, {Hostname: "host2", Services: kvOnlyService},
						},
					},
				},
			},
			service:  ServiceManagement,
			expected: []string{"https://host1:18091"},
		},
		{
			name: "MultiNodeAllServicesAltAddr",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				useAltAddr: true,
				manager: &ClusterConfigManager{
					config: &ClusterConfig{
						Nodes: Nodes{
							{
								Hostname: "host1",
								Services: testServices,
								AlternateAddresses: AlternateAddresses{
									External: &External{Hostname: "althost1", Services: testAltServices},
								},
							},
							{
								Hostname: "host2",
								Services: kvOnlyService,
							},
						},
					},
				},
			},
			service:  ServiceManagement,
			expected: []string{"http://althost1:8092"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.provider.GetAllServiceHosts(test.service)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestAuthProviderGetAllServiceHostsServiceNotAvailable(t *testing.T) {
	services := &Services{
		Management:    8091,
		ManagementSSL: 18091,
	}

	provider := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
		},
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: Nodes{{Hostname: "localhost", Services: services}},
			},
		},
	}

	_, err := provider.GetAllServiceHosts(ServiceData)
	require.NotNil(t, err)

	var errServiceNotAvailable *ServiceNotAvailableError

	require.ErrorAs(t, err, &errServiceNotAvailable)
}

func TestAuthProviderGetHostServiceNotAvailable(t *testing.T) {
	provider := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
		},
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: Nodes{{Hostname: "localhost", Services: &Services{}}},
			},
		},
	}

	_, err := provider.GetServiceHost(ServiceAnalytics)

	var errServiceNotAvailable *ServiceNotAvailableError

	require.ErrorAs(t, err, &errServiceNotAvailable)
}

func TestAuthProviderGetHostServiceShouldUseBootstrapHost(t *testing.T) {
	t.Run("BootstrapNodeIsRunningService", func(t *testing.T) {
		provider := &AuthProvider{
			resolved: &connstr.ResolvedConnectionString{
				Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
			},
			manager: &ClusterConfigManager{
				config: &ClusterConfig{
					Nodes: Nodes{
						{Hostname: "localhost", Services: &Services{CBAS: 12345}},
						{Hostname: "bootstrap", Services: &Services{CBAS: 54321}, BootstrapNode: true},
					},
				},
			},
		}

		hostname, err := provider.GetServiceHost(ServiceAnalytics)
		require.NoError(t, err)
		require.Equal(t, hostname, "http://bootstrap:54321")
	})

	t.Run("BootstrapNodeNotRunningService", func(t *testing.T) {
		provider := &AuthProvider{
			resolved: &connstr.ResolvedConnectionString{
				Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
			},
			manager: &ClusterConfigManager{
				config: &ClusterConfig{
					Nodes: Nodes{
						{Hostname: "localhost", Services: &Services{CBAS: 12345}},
						{Hostname: "bootstrap", Services: &Services{}, BootstrapNode: true},
					},
				},
			},
		}

		hostname, err := provider.GetServiceHost(ServiceAnalytics)
		require.NoError(t, err)
		require.Equal(t, hostname, "http://localhost:12345")
	})
}

func TestAuthProviderGetCredentials(t *testing.T) {
	type test struct {
		name             string
		provider         *AuthProvider
		host             string
		expectedUsername string
		expectedPassword string
	}

	tests := []*test{
		{
			name: "StandardPassword",
			provider: &AuthProvider{
				provider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
			},
			host:             "hostname",
			expectedUsername: "username",
			expectedPassword: "password",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			username, password := test.provider.GetCredentials(test.host)
			require.Equal(t, test.expectedUsername, username)
			require.Equal(t, test.expectedPassword, password)
		})
	}
}

func TestAuthProviderSuccessiveBootstrapping(t *testing.T) {
	type test struct {
		name     string
		input    *connstr.ResolvedConnectionString
		expected []string
	}

	tests := []*test{
		{
			name: "SingleHost",
			input: &connstr.ResolvedConnectionString{
				Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
			},
			expected: []string{"http://localhost:8091"},
		},
		{
			name: "MultipleHosts",
			input: &connstr.ResolvedConnectionString{
				Addresses: []connstr.Address{{Host: "hostname1", Port: 8091}, {Host: "hostname2", Port: 8091}},
			},
			expected: []string{"http://hostname1:8091", "http://hostname2:8091"},
		},
		{
			name: "MultipleHostsSSL",
			input: &connstr.ResolvedConnectionString{
				Addresses: []connstr.Address{{Host: "hostname1", Port: 8091}, {Host: "hostname2", Port: 8091}},
				UseSSL:    true,
			},
			expected: []string{"https://hostname1:8091", "https://hostname2:8091"},
		},
		{
			name: "MultipleHostsMixedStyle",
			input: &connstr.ResolvedConnectionString{
				Addresses: []connstr.Address{{Host: "hostname1", Port: 8091}, {Host: "[::1]", Port: 8091}},
				UseSSL:    true,
			},
			expected: []string{"https://hostname1:8091", "https://[::1]:8091"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				hosts    = make([]string, 0)
				provider = &AuthProvider{resolved: test.input}
				hostFunc = provider.bootstrapHostFunc()
			)

			for {
				host := hostFunc()
				if host == "" {
					break
				}

				hosts = append(hosts, host)
			}

			require.Equal(t, test.expected, hosts)
		})
	}
}
