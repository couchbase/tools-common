package auth

import (
	"os"
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/require"
)

func TestNewGocbAuthProviderLoadMappings(t *testing.T) {
	require.NoError(t, os.Setenv("CBM_SERVICES_KV_HOSTS", "localhost:8091=password1,10.111.112.131:8091=password2"))
	defer os.Unsetenv("CBM_SERVICES_KV_HOSTS")

	expected := &GocbProvider{
		baseProvider{
			username: BackupServiceUser,
			password: "password",
			mappings: HostMappings{
				"localhost:8091":      "password1",
				"10.111.112.131:8091": "password2",
			},
		},
	}

	require.Equal(t, NewGocbProvider(BackupServiceUser, "password"), expected)
}

func TestGocbAuthProvider(t *testing.T) {
	require.NoError(t, os.Setenv("CBM_SERVICES_KV_HOSTS", "localhost:8091=password1,10.111.112.131:8091=password2"))
	defer os.Unsetenv("CBM_SERVICES_KV_HOSTS")

	type test struct {
		name     string
		username string
		password string
		request  gocb.AuthCredsRequest
		expected []gocb.UserPassPair
	}

	tests := []*test{
		{
			name:     "MemcachedKnownHost",
			username: BackupServiceUser,
			password: "notthepassword",
			request:  gocb.AuthCredsRequest{Service: gocb.ServiceTypeKeyValue, Endpoint: "localhost:8091"},
			expected: []gocb.UserPassPair{{Username: BackupServiceUser, Password: "password1"}},
		},
		{
			name:     "MemcachedUnknownHost",
			username: BackupServiceUser,
			password: "password",
			request:  gocb.AuthCredsRequest{Service: gocb.ServiceTypeKeyValue, Endpoint: "notanendpoint:8091"},
			expected: []gocb.UserPassPair{{Username: BackupServiceUser, Password: "password"}},
		},
		{
			name:     "Management",
			username: BackupServiceUser,
			password: "password",
			request:  gocb.AuthCredsRequest{Service: gocb.ServiceTypeManagement, Endpoint: "10.111.112.131:8091"},
			expected: []gocb.UserPassPair{{Username: BackupServiceUser, Password: "password2"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credentials, err := NewGocbProvider(test.username, test.password).Credentials(test.request)
			require.NoError(t, err)
			require.Equal(t, credentials, test.expected)
		})
	}
}
