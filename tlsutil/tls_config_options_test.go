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
			config: TLSConfigOptions{ServerCAs: []byte("certdata")},
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
				ClientCert: []byte("certdata"),
				ClientKey:  []byte("keydata"),
			},
			valid: true,
		},
		{
			name: "PKCS#8",
			config: TLSConfigOptions{
				ClientCert: []byte("certdata"),
				ClientKey:  []byte("keydata"),
				Password:   []byte("asdasd"),
			},
			valid: true,
		},
		{
			name: "PKCS#12",
			config: TLSConfigOptions{
				ClientCert: []byte("certdata"),
				Password:   []byte("asdasd"),
			},
			valid: true,
		},
		{
			name:   "ExpectAKeyOrPassword",
			config: TLSConfigOptions{ClientCert: []byte("certdata")},
		},
		{
			name:   "PasswordOnItsOwn",
			config: TLSConfigOptions{Password: []byte("asdasd")},
		},
		{
			name:   "KeyOnItsOwn",
			config: TLSConfigOptions{ClientKey: []byte("keydata")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.valid, test.config.Validate() == nil)
		})
	}
}
