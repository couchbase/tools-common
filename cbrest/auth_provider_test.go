package cbrest

import (
	"errors"
	"os"
	"testing"

	"github.com/couchbase/tools-common/auth"
	"github.com/couchbase/tools-common/connstr"

	"github.com/stretchr/testify/require"
)

func TestNewAuthProvider(t *testing.T) {
	expected := &AuthProvider{
		resolved:  &connstr.ResolvedConnectionString{},
		userAgent: "user-agent",
		username:  "username",
		password:  "password",
		mappings:  make(auth.HostMappings),
	}

	require.Equal(t, expected,
		NewAuthProvider(&connstr.ResolvedConnectionString{}, "username", "password", "user-agent"))
}

func TestNewAuthProviderWithHostMappings(t *testing.T) {
	os.Setenv("CBM_SERVICES_KV_HOSTS", "172.20.1.1:8091=password")
	defer os.Unsetenv("CBM_SERVICES_KV_HOSTS")

	expected := &AuthProvider{
		resolved:  &connstr.ResolvedConnectionString{},
		userAgent: "user-agent",
		username:  "username",
		password:  "password",
		mappings: auth.HostMappings{
			"172.20.1.1:8091": "password",
		},
	}

	require.Equal(t, expected,
		NewAuthProvider(&connstr.ResolvedConnectionString{}, "username", "password", "user-agent"))
}

func TestNewCouchbaseGetFallbackHost(t *testing.T) {
	provider := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{Addresses: []connstr.Address{{Host: "hostname"}}},
	}

	require.Equal(t, "hostname", provider.GetFallbackHost())
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
			name: "UseDefaultHostToBootStrap",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
			},
			service:  ServiceManagement,
			expected: "http://localhost:8091",
		},
		{
			name: "UseDefaultHostToBootStrapUseSSL",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 18091}},
					UseSSL:    true,
				},
			},
			service:  ServiceManagement,
			expected: "https://localhost:18091",
		},
		{
			name: "SingleNode",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				nodes: Nodes{{Hostname: "localhost", Services: testServices}},
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
				nodes: Nodes{{Hostname: "localhost", Services: testServices}},
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
				nodes: Nodes{{
					Hostname: "localhost",
					Services: testServices,
					AlternateAddresses: AlternateAddresses{
						External: &External{Hostname: "hostname", Services: testAltServices},
					},
				}},
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
				nodes: Nodes{
					{
						Hostname: "localhost",
						Services: testServices,
						AlternateAddresses: AlternateAddresses{
							External: &External{
								Hostname: "hostname",
								Services: testAltServices,
							},
						},
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
		name     string
		provider *AuthProvider
		service  Service
		expected []string
	}

	tests := []*test{
		{
			name: "UseDefaultHostNotBootStrapped",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
			},
			service:  ServiceManagement,
			expected: []string{"http://localhost:8091"},
		},
		{
			name: "UseDefaultHostToBootStrapUseSSL",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 18091}},
					UseSSL:    true,
				},
			},
			service:  ServiceManagement,
			expected: []string{"https://localhost:18091"},
		},
		{
			name: "SingleNodeAllServices",
			provider: &AuthProvider{
				resolved: &connstr.ResolvedConnectionString{
					Addresses: []connstr.Address{{Host: "localhost", Port: 8091}},
				},
				nodes: Nodes{{Hostname: "localhost", Services: testServices}},
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
				nodes: Nodes{{Hostname: "host1", Services: testServices}, {Hostname: "host2", Services: testServices}},
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
				nodes: Nodes{{Hostname: "host1", Services: testServices}, {Hostname: "host2", Services: kvOnlyService}},
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
				nodes: Nodes{{Hostname: "host1", Services: testServices}, {Hostname: "host2", Services: kvOnlyService}},
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
				nodes: Nodes{
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
			service:  ServiceManagement,
			expected: []string{"http://althost1:8092"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.provider.GetAllServiceHosts(test.service)
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
		nodes: Nodes{{Hostname: "localhost", Services: services}},
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
		nodes: Nodes{{Hostname: "localhost", Services: &Services{}}},
	}

	_, err := provider.GetServiceHost(ServiceAnalytics)

	var errServiceNotAvailable *ServiceNotAvailableError

	require.ErrorAs(t, err, &errServiceNotAvailable)
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
			name:             "StandardPassword",
			provider:         &AuthProvider{username: "username", password: "password"},
			host:             "hostname",
			expectedUsername: "username",
			expectedPassword: "password",
		},
		{
			name: "BackupUserMappedPassword",
			provider: &AuthProvider{
				username: auth.BackupServiceUser,
				password: "password",
				mappings: map[string]string{"hostname": "mapped_password"},
			},
			host:             "hostname",
			expectedUsername: auth.BackupServiceUser,
			expectedPassword: "mapped_password",
		},
		{
			name: "BackupUserFallbackPassword",
			provider: &AuthProvider{
				username: auth.BackupServiceUser,
				password: "password",
				mappings: map[string]string{},
			},
			host:             "hostname",
			expectedUsername: auth.BackupServiceUser,
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
			hosts := make([]string, 0)
			provider := &AuthProvider{resolved: test.input}

			for {
				host, err := provider.GetServiceHost(ServiceManagement)
				if err != nil {
					if errors.Is(err, errExhaustedBootstrapHosts) {
						break
					}

					t.Fatalf("Expected to be able to get host: %v", err)
				}

				hosts = append(hosts, host)
			}

			require.Equal(t, test.expected, hosts)
		})
	}
}
