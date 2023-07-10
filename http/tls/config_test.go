package tls

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

func TestNewConfigMiscOptions(t *testing.T) {
	config, err := NewConfig(ConfigOptions{
		ClientAuthType: tls.VerifyClientCertIfGiven,
		CipherSuites:   []uint16{8, 16, 32, 64, 128},
		MinVersion:     42,
	})
	require.NoError(t, err)
	require.Equal(t, tls.VerifyClientCertIfGiven, config.ClientAuth)
	require.Equal(t, []uint16{8, 16, 32, 64, 128}, config.CipherSuites)
	require.Equal(t, uint16(42), config.MinVersion)
}

func TestNewConfigValidClientKeyPair(t *testing.T) {
	config, err := NewConfig(ConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewConfigInvalidClientKeyPair(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     invalidCertPEM,
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewConfigClientCAs(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		config, err := NewConfig(ConfigOptions{
			ClientAuthType: tls.VerifyClientCertIfGiven,
			ClientCAs:      validCertPEM,
		})
		require.NoError(t, err)
		require.Len(t, config.ClientCAs.Subjects(), 1) //nolint:staticcheck
		require.Nil(t, config.Certificates)
	})

	t.Run("Disabled", func(t *testing.T) {
		config, err := NewConfig(ConfigOptions{
			ClientAuthType: tls.NoClientCert,
			ClientCAs:      validCertPEM,
		})
		require.NoError(t, err)
		require.Nil(t, config.ClientCAs)
		require.Nil(t, config.Certificates)
	})

	t.Run("EnabledButMissing", func(t *testing.T) {
		config, err := NewConfig(ConfigOptions{ClientAuthType: tls.VerifyClientCertIfGiven})
		require.NoError(t, err)
		require.Nil(t, config.ClientCAs)
		require.Nil(t, config.Certificates)
	})
}

func TestNewConfigValidRootCAs(t *testing.T) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}

	config, err := NewConfig(ConfigOptions{RootCAs: validCertPEM})
	require.NoError(t, err)
	require.False(t, config.InsecureSkipVerify)
	require.Len(t, config.RootCAs.Subjects(), len(pool.Subjects())+1) //nolint:staticcheck
	require.Nil(t, config.Certificates)

	config, err = NewConfig(ConfigOptions{})
	require.NoError(t, err)
	require.False(t, config.InsecureSkipVerify)
	require.Nil(t, config.RootCAs)
	require.Nil(t, config.Certificates)

	config, err = NewConfig(ConfigOptions{RootCAs: validCertPEM, NoSSLVerify: true})
	require.NoError(t, err)
	require.True(t, config.InsecureSkipVerify)
	require.Nil(t, config.RootCAs)
	require.Nil(t, config.Certificates)
}

func TestNewConfigInvalidCert(t *testing.T) {
	_, err := NewConfig(ConfigOptions{RootCAs: invalidCertPEM})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewConfigEmptyCert(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     make([]byte, 0),
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewConfigInvalidKey(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      invalidKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewConfigEmptyKey(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      make([]byte, 0),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewConfigUnencryptedWithPassword(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      validKeyPEM,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrPasswordProvidedButUnused)
}

func TestNewConfigUnencryptedWithPasswordIgnoreUnused(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:           validCertPEM,
		ClientKey:            validKeyPEM,
		Password:             []byte("asdasd"),
		IgnoreUnusedPassword: true,
		ClientAuthType:       tls.VerifyClientCertIfGiven,
	})
	require.Nil(t, err)
}

func TestNewConfigValidEncryptedPKCS12(t *testing.T) {
	config, err := NewConfig(ConfigOptions{
		ClientCert:     validCertAndKeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
	require.IsType(t, config.Certificates[0].PrivateKey, &rsa.PrivateKey{})
}

func TestNewConfigValidEncryptedPKCS12WrongPassword(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertAndKeyPKCS12,
		Password:       []byte("not-the-password"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewConfigValidEncryptedPKCS12WithoutPassword(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertAndKeyPKCS12,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.Error(t, err)
}

func TestNewConfigInvalidEncryptedPKCS12(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     invalidCertAndKeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.True(t, parseCertKeyError.password)
}

func TestNewConfigValidEncryptedPKCS8(t *testing.T) {
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
			config, err := NewConfig(ConfigOptions{
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

func TestNewConfigValidEncryptedPKCS8WrongPassword(t *testing.T) {
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
			_, err := NewConfig(ConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("not-the-password"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
		})
	}
}

func TestNewConfigValidEncryptedPKCS8WithoutPassword(t *testing.T) {
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
			_, err := NewConfig(ConfigOptions{
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

func TestNewConfigInvalidEncryptedPKCS8(t *testing.T) {
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
			_, err := NewConfig(ConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("asdasd"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
		})
	}
}

func TestNewConfigMultipleCertsPEM(t *testing.T) {
	config, err := NewConfig(ConfigOptions{
		ClientCert:     validCertsPEM,
		ClientKey:      validKeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.Len(t, config.Certificates[0].Certificate, 3)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewConfigMultipleCertsPKCS12(t *testing.T) {
	config, err := NewConfig(ConfigOptions{
		ClientCert:     validCertsAndKeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.Len(t, config.Certificates[0].Certificate, 3)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewConfigMismatchedKeys(t *testing.T) {
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
			_, err := NewConfig(ConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.key,
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPublicPrivateKeyPair)
		})
	}
}

func TestNewConfigUnsupportedPublicPrivateKeyUnencrypted(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertPEM,
		ClientKey:      validED488KeyPEM,
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewConfigUnsupportedPublicPrivateKeyPKCS12(t *testing.T) {
	_, err := NewConfig(ConfigOptions{
		ClientCert:     validCertAndED488KeyPKCS12,
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewConfigUnsupportedPublicPrivateKeyPKCS8(t *testing.T) {
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
			_, err := NewConfig(ConfigOptions{
				ClientCert:     validCertPEM,
				ClientKey:      test.data,
				Password:       []byte("asdasd"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
		})
	}
}
