package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCredentials(t *testing.T) {
	type test struct {
		name             string
		username         string
		password         string
		endpoint         string
		mappings         HostMappings
		expectedUsername string
		expectedPassword string
	}

	tests := []*test{
		{
			name:             "NormalUser",
			username:         "Administrator",
			password:         "asdasd",
			endpoint:         "localhost:8091",
			expectedUsername: "Administrator",
			expectedPassword: "asdasd",
		},
		{
			name:     "BackupServiceUserKnownHost",
			username: BackupServiceUser,
			password: "password",
			endpoint: "localhost:8091",
			mappings: HostMappings{
				"localhost:8091": "mapped_password",
			},
			expectedUsername: BackupServiceUser,
			expectedPassword: "mapped_password",
		},
		{
			name:     "BackupServiceUserUnknownHost",
			username: BackupServiceUser,
			password: "password",
			endpoint: "localhost:8091",
			mappings: HostMappings{
				"another_host:8091": "mapped_password",
			},
			expectedUsername: BackupServiceUser,
			expectedPassword: "password",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			username, password := GetCredentials(test.username, test.password, test.endpoint, test.mappings)
			require.Equal(t, test.expectedUsername, username)
			require.Equal(t, test.expectedPassword, password)
		})
	}
}
