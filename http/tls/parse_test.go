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

// generateCA returns a CA cert with the given subject, signed by parent and its key, or self-signed when parent is nil.
func generateCA(t *testing.T, subject pkix.Name, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (
	*x509.Certificate, *ecdsa.PrivateKey,
) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               subject,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	if parent == nil {
		parent, parentKey = template, priv
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parent, &priv.PublicKey, parentKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert, priv
}

func encode(certs ...*x509.Certificate) []byte {
	var data []byte
	for _, cert := range certs {
		data = append(data, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})...)
	}

	return data
}

func TestParseCerts(t *testing.T) {
	var (
		subject    = pkix.Name{Organization: []string{"Couchbase"}}
		root, key  = generateCA(t, subject, nil, nil)
		sub, _     = generateCA(t, subject, root, key)
		_, privKey = generatePair(t)
	)

	tests := []struct {
		name    string
		data    []byte
		want    []*x509.Certificate
		wantErr bool
	}{
		{name: "single cert", data: encode(root), want: []*x509.Certificate{root}},
		{name: "keeps order", data: encode(sub, root), want: []*x509.Certificate{sub, root}},
		{
			name: "text between blocks",
			data: slices.Concat(encode(sub), []byte("comment\n"), encode(root)),
			want: []*x509.Certificate{sub, root},
		},
		{name: "empty", data: nil, wantErr: true},
		{name: "malformed block between certs", data: slices.Concat(encode(sub), malformed, encode(root)), wantErr: true},
		{name: "malformed leading block", data: slices.Concat(malformed, encode(root)), wantErr: true},
		{name: "trailing data", data: slices.Concat(encode(root), []byte("garbage")), wantErr: true},
		{name: "non-cert block", data: slices.Concat(encode(root), privKey), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			certs, err := ParseCerts(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, certs, len(tt.want))

			for i := range tt.want {
				require.Equal(t, tt.want[i].Raw, certs[i].Raw)
			}
		})
	}
}

func TestIsSelfSigned(t *testing.T) {
	var (
		subject     = pkix.Name{Organization: []string{"Couchbase"}}
		root, key   = generateCA(t, subject, nil, nil)
		other, oKey = generateCA(t, pkix.Name{Organization: []string{"Other"}}, nil, nil)
		sub, _      = generateCA(t, subject, root, key)
		cross, _    = generateCA(t, subject, &x509.Certificate{Subject: subject}, oKey)
		issued, _   = generateCA(t, pkix.Name{CommonName: "leaf"}, root, key)
		renamed     = selfSignedAs(t, subject, pkix.Name{Organization: []string{"Other"}})
	)

	tests := []struct {
		name string
		cert *x509.Certificate
		want bool
	}{
		{name: "root", cert: root, want: true},
		{name: "another root", cert: other, want: true},
		{name: "subordinate sharing the root's subject", cert: sub},
		{name: "issuer matches subject but signed by another key", cert: cross},
		{name: "issued by root", cert: issued},
		{name: "signed by its own key but issuer differs from subject", cert: renamed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsSelfSigned(tt.cert))
		})
	}
}

// selfSignedAs returns a cert with the given subject, signed by its own key but naming issuer as its issuer.
func selfSignedAs(t *testing.T, subject, issuer pkix.Name) *x509.Certificate {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               subject,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, &x509.Certificate{Subject: issuer}, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert
}
