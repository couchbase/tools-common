package auth

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetHostMappings(t *testing.T) {
	defer os.Unsetenv("CBM_SERVICES_KV_HOSTS")

	t.Run("valid", func(t *testing.T) {
		require.NoError(t, os.Setenv("CBM_SERVICES_KV_HOSTS", "localhost:8091=password1,10.111.112.131:8091=password2,"+
			"some-host-name.com:8091=password3"))

		mappings := GetHostMappings()

		expected := HostMappings{
			"localhost:8091":          "password1",
			"10.111.112.131:8091":     "password2",
			"some-host-name.com:8091": "password3",
		}

		require.Equal(t, mappings, expected)
	})

	t.Run("invalidVars", func(t *testing.T) {
		require.NoError(t, os.Setenv("CBM_SERVICES_KV_HOSTS", "localhost:8091,,some-host-name.com:8091=password3"))

		expected := HostMappings{
			"some-host-name.com:8091": "password3",
		}

		mappings := GetHostMappings()

		require.Equal(t, mappings, expected)
	})
}

func TestHostMappings_GetPassword(t *testing.T) {
	mappings := HostMappings{
		"localhost:8091":          "password1",
		"10.111.112.131:8091":     "password2",
		"some-host-name.com:8091": "password3",
	}

	type test struct {
		name        string
		search      string
		password    string
		expectError bool
	}

	tests := []*test{
		{
			name:     "ip-match",
			search:   "10.111.112.131:8091",
			password: "password2",
		},
		{
			name:     "domain-name-match",
			search:   "some-host-name.com:8091",
			password: "password3",
		},
		{
			name:     "localhost-match",
			search:   "localhost:8091",
			password: "password1",
		},
		{
			name:     "localhost-equivalent-match",
			search:   "127.0.0.1:8091",
			password: "password1",
		},
		{
			name:     "with-schema",
			search:   "http://[::1]:8091",
			password: "password1",
		},
		{
			name:        "host-but-not-port-match",
			search:      "10.111.112.131:9000",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			password, err := mappings.GetPassword(test.search)
			require.Equal(t, test.expectError, err != nil)
			require.Equal(t, test.password, password)
		})
	}
}
