package tlsutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTLSConfigOptionsValidate(t *testing.T) {
	type test struct {
		name   string
		config TLSConfigOptions
		valid  bool
	}

	tests := []*test{
		{
			name:  "Empty",
			valid: true,
		},
		{
			name:   "ServerCAs",
			config: TLSConfigOptions{ServerCAs: "/path/to/certs"},
			valid:  true,
		},
		{
			name:   "NoSSLVerify",
			config: TLSConfigOptions{NoSSLVerify: true},
			valid:  true,
		},
		{
			name: "UnencryptedKey",
			config: TLSConfigOptions{
				ClientCert: "/path/to/certs",
				ClientKey:  "/path/to/key",
			},
			valid: true,
		},
		{
			name: "PKCS#8",
			config: TLSConfigOptions{
				ClientCert: "/path/to/certs",
				ClientKey:  "/path/to/key",
				Password:   []byte("asdasd"),
			},
			valid: true,
		},
		{
			name: "PKCS#12",
			config: TLSConfigOptions{
				ClientCert: "/path/to/certs",
				Password:   []byte("asdasd"),
			},
			valid: true,
		},
		{
			name:   "ExpectAKeyOrPassword",
			config: TLSConfigOptions{ClientCert: "/path/to/certs"},
		},
		{
			name:   "PasswordOnItsOwn",
			config: TLSConfigOptions{Password: []byte("asdasd")},
		},
		{
			name:   "KeyOnItsOwn",
			config: TLSConfigOptions{ClientKey: "/path/to/a/key"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.valid, test.config.Validate() == nil)
		})
	}
}
