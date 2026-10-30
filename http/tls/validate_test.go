package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// malformed is a PEM block whose contents are not valid base64 DER.
var malformed = []byte("-----BEGIN CERTIFICATE-----\ngarbage\n-----END CERTIFICATE-----\n")

// generatePair returns a PEM encoded self-signed cert and its matching private key.
func generatePair(t *testing.T) ([]byte, []byte) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	privder, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)

	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	key := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privder})

	return cert, key
}

func TestValidateCA(t *testing.T) {
	var (
		cert, key = generatePair(t)
		other, _  = generatePair(t)
		text      = []byte("comment\n")
		garbage   = []byte("garbage")
	)

	tests := []struct {
		name    string
		ca      []byte
		wantErr bool
	}{
		{name: "single cert", ca: cert},
		{name: "bundle", ca: slices.Concat(cert, other)},
		{name: "surrounding whitespace", ca: slices.Concat([]byte("\n"), cert, []byte("\n\n"))},
		{name: "leading text", ca: slices.Concat(text, cert)},
		{name: "text between blocks", ca: slices.Concat(cert, text, other)},
		{name: "malformed PEM between blocks", ca: slices.Concat(cert, malformed, other), wantErr: true},
		{name: "empty", ca: nil, wantErr: true},
		{name: "garbage", ca: garbage, wantErr: true},
		{name: "malformed trailing PEM", ca: slices.Concat(cert, malformed), wantErr: true},
		{name: "malformed leading PEM", ca: slices.Concat(malformed, cert), wantErr: true},
		{name: "trailing data", ca: slices.Concat(cert, garbage), wantErr: true},
		{name: "non-cert block", ca: slices.Concat(cert, key), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCA(tt.ca)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateKeyPair(t *testing.T) {
	var (
		cert, key   = generatePair(t)
		_, otherKey = generatePair(t)
		garbage     = []byte("garbage")
	)

	tests := []struct {
		name    string
		cert    []byte
		key     []byte
		wantErr bool
	}{
		{name: "valid", cert: cert, key: key},
		{name: "empty cert", cert: nil, key: key, wantErr: true},
		{name: "empty key", cert: cert, key: nil, wantErr: true},
		{name: "garbage", cert: garbage, key: garbage, wantErr: true},
		{name: "mismatched", cert: cert, key: otherKey, wantErr: true},
		{name: "cert with malformed trailing PEM", cert: slices.Concat(cert, malformed), key: key, wantErr: true},
		{name: "key with malformed trailing PEM", cert: cert, key: slices.Concat(key, malformed), wantErr: true},
		{name: "key is a cert", cert: cert, key: cert, wantErr: true},
		{name: "multiple keys", cert: cert, key: slices.Concat(key, key), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKeyPair(tt.cert, tt.key)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}
