package tlsutil

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"path/filepath"
	"testing"

	"github.com/couchbase/tools-common/fsutil"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/valid_cert.pem
	validCertPEM []byte

	//go:embed testdata/valid_certs.pem
	validCertsPEM []byte

	//go:embed testdata/valid_key.pem
	validKeyPEM []byte

	//go:embed testdata/valid_key.p8
	validKeyPKCS8 []byte

	//go:embed testdata/invalid_key.p8
	invalidKeyPKCS8 []byte

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

	//go:embed testdata/valid_ed448_key.p8
	validED488KeyPKCS8 []byte

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
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), validKeyPEM, 0))

	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewTLSConfigInvalidClientKeyPair(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), invalidCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), validKeyPEM, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigClientCAs(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))

	t.Run("Valid", func(t *testing.T) {
		config, err := NewTLSConfig(TLSConfigOptions{
			ClientAuthType: tls.VerifyClientCertIfGiven,
			ClientCAs:      filepath.Join(testDir, "cert.pem"),
		})
		require.NoError(t, err)
		require.Len(t, config.ClientCAs.Subjects(), 1)
		require.Nil(t, config.Certificates)
	})

	t.Run("Disabled", func(t *testing.T) {
		config, err := NewTLSConfig(TLSConfigOptions{
			ClientAuthType: tls.NoClientCert,
			ClientCAs:      filepath.Join(testDir, "cert.pem"),
		})
		require.NoError(t, err)
		require.Len(t, config.ClientCAs.Subjects(), 0)
		require.Nil(t, config.Certificates)
	})

	t.Run("EnabledButMissing", func(t *testing.T) {
		config, err := NewTLSConfig(TLSConfigOptions{ClientAuthType: tls.VerifyClientCertIfGiven})
		require.NoError(t, err)
		require.Len(t, config.ClientCAs.Subjects(), 0)
		require.Nil(t, config.Certificates)
	})
}

func TestNewTLSConfigValidServerCAs(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))

	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}

	config, err := NewTLSConfig(TLSConfigOptions{ServerCAs: filepath.Join(testDir, "cert.pem")})
	require.NoError(t, err)
	require.False(t, config.InsecureSkipVerify)
	require.Len(t, config.RootCAs.Subjects(), len(pool.Subjects())+1)
	require.Nil(t, config.Certificates)

	config, err = NewTLSConfig(TLSConfigOptions{ServerCAs: filepath.Join(testDir, "cert.pem"), NoSSLVerify: true})
	require.NoError(t, err)
	require.True(t, config.InsecureSkipVerify)
	require.Equal(t, config.RootCAs.Subjects(), pool.Subjects())
	require.Nil(t, config.Certificates)
}

func TestNewTLSConfigInvalidCert(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), invalidCertPEM, 0))

	_, err := NewTLSConfig(TLSConfigOptions{ServerCAs: filepath.Join(testDir, "cert.pem")})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigEmptyCert(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.Touch(filepath.Join(testDir, "cert.pem")))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), validKeyPEM, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigInvalidKey(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), invalidKeyPEM, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigEmptyKey(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.Touch(filepath.Join(testDir, "key.pem")))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigUnencryptedWithPassword(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), validKeyPEM, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrPasswordProvidedButUnused)
}

func TestNewTLSConfigValidEncryptedPKCS12(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert_and_key.p12"), validCertAndKeyPKCS12, 0))

	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert_and_key.p12"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
	require.IsType(t, config.Certificates[0].PrivateKey, &rsa.PrivateKey{})
}

func TestNewTLSConfigValidEncryptedPKCS12WrongPassword(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert_and_key.p12"), validCertAndKeyPKCS12, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert_and_key.p12"),
		Password:       []byte("not-the-password"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigValidEncryptedPKCS12WithoutPassword(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert_and_key.p12"), validCertAndKeyPKCS12, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert_and_key.p12"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.Error(t, err)
}

func TestNewTLSConfigInvalidEncryptedPKCS12(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert_and_key.p12"), invalidCertAndKeyPKCS12, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert_and_key.p12"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "certificates", parseCertKeyError.what)
	require.True(t, parseCertKeyError.password)
}

func TestNewTLSConfigValidEncryptedPKCS8(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.p8"), validKeyPKCS8, 0))

	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.p8"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.NotNil(t, config.Certificates[0].Leaf)
	require.IsType(t, config.Certificates[0].PrivateKey, &rsa.PrivateKey{})
}

func TestNewTLSConfigValidEncryptedPKCS8WrongPassword(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.p8"), validKeyPKCS8, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.p8"),
		Password:       []byte("not-the-password"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigValidEncryptedPKCS8WithoutPassword(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.p8"), validKeyPKCS8, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.p8"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})

	var parseCertKeyError ParseCertKeyError

	require.ErrorAs(t, err, &parseCertKeyError)
	require.Equal(t, "private key", parseCertKeyError.what)
	require.False(t, parseCertKeyError.password)
}

func TestNewTLSConfigInvalidEncryptedPKCS8(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.p8"), invalidKeyPKCS8, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.p8"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigMultipleCertsPEM(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "certs.pem"), validCertsPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), validKeyPEM, 0))

	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "certs.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.Len(t, config.Certificates[0].Certificate, 3)
	require.NotNil(t, config.Certificates[0].Leaf)
}

func TestNewTLSConfigMultipleCertsPKCS12(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "certs_and_key.p12"), validCertsAndKeyPKCS12, 0))

	config, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "certs_and_key.p12"),
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
			testDir := t.TempDir()

			require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
			require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), test.key, 0))

			_, err := NewTLSConfig(TLSConfigOptions{
				ClientCert:     filepath.Join(testDir, "cert.pem"),
				ClientKey:      filepath.Join(testDir, "key.pem"),
				ClientAuthType: tls.VerifyClientCertIfGiven,
			})
			require.ErrorIs(t, err, ErrInvalidPublicPrivateKeyPair)
		})
	}
}

func TestNewTLSConfigUnsupportedPublicPrivateKeyUnencrypted(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.pem"), validED488KeyPEM, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.pem"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigUnsupportedPublicPrivateKeyPKCS12(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertAndED488KeyPKCS12, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}

func TestNewTLSConfigUnsupportedPublicPrivateKeyPKCS8(t *testing.T) {
	testDir := t.TempDir()

	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "cert.pem"), validCertPEM, 0))
	require.NoError(t, fsutil.WriteFile(filepath.Join(testDir, "key.p8"), validED488KeyPKCS8, 0))

	_, err := NewTLSConfig(TLSConfigOptions{
		ClientCert:     filepath.Join(testDir, "cert.pem"),
		ClientKey:      filepath.Join(testDir, "key.p8"),
		Password:       []byte("asdasd"),
		ClientAuthType: tls.VerifyClientCertIfGiven,
	})
	require.ErrorIs(t, err, ErrInvalidPasswordInputDataOrKey)
}
