package tls

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigOptionsValidate(t *testing.T) {
	type test struct {
		name   string
		config ConfigOptions
		valid  bool
	}

	tests := []*test{
		{
			name:  "Empty",
			valid: true,
		},
		{
			name:   "ServerCAs",
			config: ConfigOptions{RootCAs: []byte("certdata")},
			valid:  true,
		},
		{
			name:   "NoSSLVerify",
			config: ConfigOptions{NoSSLVerify: true},
			valid:  true,
		},
		{
			name: "UnencryptedKey",
			config: ConfigOptions{
				ClientCert: []byte("certdata"),
				ClientKey:  []byte("keydata"),
			},
			valid: true,
		},
		{
			name: "PKCS#8",
			config: ConfigOptions{
				ClientCert: []byte("certdata"),
				ClientKey:  []byte("keydata"),
				Password:   []byte("asdasd"),
			},
			valid: true,
		},
		{
			name: "PKCS#12",
			config: ConfigOptions{
				ClientCert: []byte("certdata"),
				Password:   []byte("asdasd"),
			},
			valid: true,
		},
		{
			name:   "ExpectAKeyOrPassword",
			config: ConfigOptions{ClientCert: []byte("certdata")},
		},
		{
			name:   "PasswordOnItsOwn",
			config: ConfigOptions{Password: []byte("asdasd")},
		},
		{
			name:   "KeyOnItsOwn",
			config: ConfigOptions{ClientKey: []byte("keydata")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.valid, test.config.Validate() == nil)
		})
	}
}
