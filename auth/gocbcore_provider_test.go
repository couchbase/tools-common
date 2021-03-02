package auth

import (
	"os"
	"testing"

	"github.com/couchbase/gocbcore/v9"
	"github.com/stretchr/testify/require"
)

func TestNewGocbcoreAuthProviderLoadMappings(t *testing.T) {
	require.NoError(t, os.Setenv("CBM_SERVICES_KV_HOSTS", "localhost:8091=password1,10.111.112.131:8091=password2"))
	defer os.Unsetenv("CBM_SERVICES_KV_HOSTS")

	expected := &GocbcoreProvider{
		baseProvider{
			username: BackupServiceUser,
			password: "password",
			mappings: HostMappings{
				"localhost:8091":      "password1",
				"10.111.112.131:8091": "password2",
			},
		},
	}

	require.Equal(t, NewGocbcoreProvider(BackupServiceUser, "password"), expected)
}

func TestGocbcoreAuthProvider(t *testing.T) {
	require.NoError(t, os.Setenv("CBM_SERVICES_KV_HOSTS", "localhost:8091=password1,10.111.112.131:8091=password2"))
	defer os.Unsetenv("CBM_SERVICES_KV_HOSTS")

	type test struct {
		name     string
		username string
		password string
		request  gocbcore.AuthCredsRequest
		expected []gocbcore.UserPassPair
	}

	tests := []*test{
		{
			name:     "MemcachedKnownHost",
			username: BackupServiceUser,
			password: "notthepassword",
			request:  gocbcore.AuthCredsRequest{Service: gocbcore.MemdService, Endpoint: "localhost:8091"},
			expected: []gocbcore.UserPassPair{{Username: BackupServiceUser, Password: "password1"}},
		},
		{
			name:     "MemcachedUnknownHost",
			username: BackupServiceUser,
			password: "password",
			request:  gocbcore.AuthCredsRequest{Service: gocbcore.MemdService, Endpoint: "notanendpoint:8091"},
			expected: []gocbcore.UserPassPair{{Username: BackupServiceUser, Password: "password"}},
		},
		{
			name:     "Management",
			username: BackupServiceUser,
			password: "password",
			request:  gocbcore.AuthCredsRequest{Service: gocbcore.MgmtService, Endpoint: "10.111.112.131:8091"},
			expected: []gocbcore.UserPassPair{{Username: BackupServiceUser, Password: "password2"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credentials, err := NewGocbcoreProvider(test.username, test.password).Credentials(test.request)
			require.NoError(t, err)
			require.Equal(t, credentials, test.expected)
		})
	}
}
