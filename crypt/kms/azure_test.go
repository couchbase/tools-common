package kms

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAzureURL(t *testing.T) {
	type testCase struct {
		name       string
		in         string
		base       string
		keyName    string
		keyVersion string
		valid      bool
	}

	cases := []testCase{
		{
			name: "not-key-vault-url",
			in:   "https://google.com",
		},
		{
			name: "vault-without-key",
			in:   "https://cbm.vault.net/keys",
		},
		{
			name:    "key-without-version",
			in:      "https://cbm.vault.net/keys/super-duper-key",
			base:    "https://cbm.vault.net/",
			keyName: "super-duper-key",
			valid:   true,
		},
		{
			name:       "key-with-version",
			in:         "https://cbm.vault.net/keys/super-duper-key/version-1",
			base:       "https://cbm.vault.net/",
			keyName:    "super-duper-key",
			keyVersion: "version-1",
			valid:      true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, keyName, keyVersion, err := parseAzureURL(tc.in)
			if !tc.valid {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.base, base)
			require.Equal(t, tc.keyName, keyName)
			require.Equal(t, tc.keyVersion, keyVersion)
		})
	}
}
