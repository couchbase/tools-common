package tlsutil

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/valid_cert.pem
	validCertPEM []byte

	//go:embed testdata/valid_certs.pem
	validCertsPEM []byte

	//go:embed testdata/valid_key.pem
	validKeyPEM []byte

	//go:embed testdata/valid_key_p8.pem
	validKeyPKCS8PEM []byte

	//go:embed testdata/valid_key_p8.der
	validKeyPKCS8DER []byte

	//go:embed testdata/invalid_key_p8.pem
	invalidKeyPKCS8PEM []byte

	//go:embed testdata/invalid_key_p8.der
	invalidKeyPKCS8DER []byte

	//go:embed testdata/valid_cert_and_key.p12
	validCertAndKeyPKCS12 []byte

	//go:embed testdata/valid_certs_and_key.p12
	validCertsAndKeyPKCS12 []byte

	//go:embed testdata/invalid_cert_and_key.p12
	invalidCertAndKeyPKCS12 []byte

	//go:embed testdata/invalid_cert.pem
	invalidCertPEM []byte

	//go:embed testdata/invalid_key.pem
	invalidKeyPEM []byte

	//go:embed testdata/valid_rsa_key.pem
	validRSAKeyPEM []byte

	//go:embed testdata/valid_ecdsa_key.pem
	validECDSAKeyPEM []byte

	//go:embed testdata/valid_ed25519_key.pem
	validE25519KeyPEM []byte

	//go:embed testdata/valid_ed448_key.pem
	validED488KeyPEM []byte

	//go:embed testdata/valid_ed448_key_p8.pem
	validED488KeyPKCS8PEM []byte

	//go:embed testdata/valid_ed448_key_p8.der
	validED488KeyPKCS8DER []byte

	//go:embed testdata/valid_cert_and_ed448_key.p12
	validCertAndED488KeyPKCS12 []byte
)

func TestNewTLSConfigMiscOptions(t *testing.T) {
	config, err := NewTLSConfig(TLSConfigOptions{
		ClientAuthType:           tls.VerifyClientCertIfGiven,
		CipherSuites:             []uint16{8, 16, 32, 64, 128},
		MinVersion:               42,
		PreferServerCipherSuites: true,
	})
	require.NoError(t, err)
	require.Equal(t, tls.VerifyClientCertIfGiven, config.ClientAuth)
	require.Equal(t, []uint16{8, 16, 32, 64, 128}, config.CipherSuites)
	require.Equal(t, uint16(42), config.MinVersion)
	require.True(t, config.PreferServerCipherSuites)
}

func TestNewTLSConfigValidClientKeyPair(t *testing.T) {
	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewTLSConfigInvalidClientKeyPair(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     invalidCertPEM,
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigClientCAs(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		config, err := NewTLSConfig(TLSConfigOptions{
			ClientAuthType: tls.VerifyClientCertIfGiven,
			ClientCAs:      validCertPEM,
		})
		require.NoError(t, err)
		require.Len(t, config.ClientCAs.Subjects(), 1)
		require.Nil(t, config.Certificates)
	})

	t.Run("Disabled", func(t *testing.T) {
		config, err := NewTLSConfig(TLSConfigOptions{
			ClientAuthType: tls.NoClientCert,
			ClientCAs:      validCertPEM,
		})
		require.NoError(t, err)
		require.Nil(t, config.ClientCAs)
		require.Nil(t, config.Certificates)
	})

	t.Run("EnabledButMissing", func(t *testing.T) {
		config, err := NewTLSConfig(TLSConfigOptions{ClientAuthType: tls.VerifyClientCertIfGiven})
		require.NoError(t, err)
		require.Nil(t, config.ClientCAs)
		require.Nil(t, config.Certificates)
	})
}

func TestNewTLSConfigValidRootCAs(t *testing.T) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}

	config, err := NewTLSConfig(TLSConfigOptions{RootCAs: validCertPEM})
	require.NoError(t, err)
	require.False(t, config.InsecureSkipVerify)
	require.Len(t, config.RootCAs.Subjects(), len(pool.Subjects())+1)
	require.Nil(t, config.Certificates)

	config, err = NewTLSConfig(TLSConfigOptions{})
	require.NoError(t, err)
	require.False(t, config.InsecureSkipVerify)
	require.Nil(t, config.RootCAs)
	require.Nil(t, config.Certificates)

	config, err = NewTLSConfig(TLSConfigOptions{RootCAs: validCertPEM, NoSSLVerify: true})
	require.NoError(t, err)
	require.True(t, config.InsecureSkipVerify)
	require.Nil(t, config.RootCAs)
	require.Nil(t, config.Certificates)
}

func TestNewTLSConfigInvalidCert(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{RootCAs: invalidCertPEM})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigEmptyCert(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     make([]byte, 0),
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigInvalidKey(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      invalidKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigEmptyKey(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      make([]byte, 0),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigUnencryptedWithPassword(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      validKeyPEM,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrPasswordProvidedButUnused)
}

func TestNewTLSConfigUnencryptedWithPasswordIgnoreUnused(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:           validCertPEM,
		ClientKey:            validKeyPEM,
		Password:             []byte("asdasd"),
		IgnoreUnusedPassword: true,
		ClientAuthType:       tls.VerifyClientCertIfGiven,
	})
	require.Nil(t, err)
}

func TestNewTLSConfigValidEncryptedPKCS12(t *testing.T) {
	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertAndKeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
	require.IsType(t, config.Certificates[0].PrivateKey, &rsa.PrivateKey{})
}

func TestNewTLSConfigValidEncryptedPKCS12WrongPassword(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertAndKeyPKCS12,
		Password:       []byte("not-the-password"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigValidEncryptedPKCS12WithoutPassword(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertAndKeyPKCS12,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.Error(t, err)
}

func TestNewTLSConfigInvalidEncryptedPKCS12(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     invalidCertAndKeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.True(t, parseCertKeyError.password)
}

func TestNewTLSConfigValidEncryptedPKCS8(t *testing.T) {
	type test struct {
		name string
		data []byte
	}

	tests := []*test{
		{
			name: "PEM",
			data: validKeyPKCS8PEM,
		},
		{
			name: "DER",
			data: validKeyPKCS8DER,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("asdasd"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.NoError(t, err)
			require.Len(t, config.Certificates, 1)
			require.NotNil(t, config.Certificates[0].Leaf)
			require.IsType(t, config.Certificates[0].PrivateKey, &rsa.PrivateKey{})
		})
	}
}

func TestNewTLSConfigValidEncryptedPKCS8WrongPassword(t *testing.T) {
	type test struct {
		name string
		data []byte
	}

	tests := []*test{
		{
			name: "PEM",
			data: validKeyPKCS8PEM,
		},
		{
			name: "DER",
			data: validKeyPKCS8DER,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("not-the-password"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
		})
	}
}

func TestNewTLSConfigValidEncryptedPKCS8WithoutPassword(t *testing.T) {
	type test struct {
		name string
		data []byte
	}

	tests := []*test{
		{
			name: "PEM",
			data: validKeyPKCS8PEM,
		},
		{
			name: "DER",
			data: validKeyPKCS8DER,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})

			var parseCertKeyError ParseCertKeyError

			require.ErrorAs(t, err, &parseCertKeyError)
			require.Equal(t, "private key", parseCertKeyError.what)
			require.False(t, parseCertKeyError.password)
		})
	}
}

func TestNewTLSConfigInvalidEncryptedPKCS8(t *testing.T) {
	type test struct {
		name string
		data []byte
	}

	tests := []*test{
		{
			name: "PEM",
			data: invalidKeyPKCS8PEM,
		},
		{
			name: "DER",
			data: invalidKeyPKCS8DER,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("asdasd"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
		})
	}
}

func TestNewTLSConfigMultipleCertsPEM(t *testing.T) {
	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertsPEM,
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.Len(t, config.Certificates[0].Certificate, 3)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewTLSConfigMultipleCertsPKCS12(t *testing.T) {
	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertsAndKeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.Len(t, config.Certificates[0].Certificate, 3)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewTLSConfigMismatchedKeys(t *testing.T) {
	type test struct {
		name string
		key  []byte
	}

	tests := []*test{
		{
			name: "RSA",
			key:  validRSAKeyPEM,
		},
		{
			name: "ECDSA",
			key:  validECDSAKeyPEM,
		},
		{
			name: "ED25519",
			key:  validE25519KeyPEM,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.key,
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPublicPrivateKeyPair)
		})
	}
}

func TestNewTLSConfigUnsupportedPublicPrivateKeyUnencrypted(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      validED488KeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigUnsupportedPublicPrivateKeyPKCS12(t *testing.T) {
	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     validCertAndED488KeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigUnsupportedPublicPrivateKeyPKCS8(t *testing.T) {
	type test struct {
		name string
		data []byte
	}

	tests := []*test{
		{
			name: "PEM",
			data: validED488KeyPKCS8PEM,
		},
		{
			name: "DER",
			data: validED488KeyPKCS8DER,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("asdasd"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
		})
	}
}
