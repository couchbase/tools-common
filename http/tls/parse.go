package tls

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// ParseCerts returns the certificates in the given data, in order, failing unless the data is strictly made of one or
// more valid PEM encoded certificates.
func ParseCerts(data []byte) ([]*x509.Certificate, error) {
	blocks, err := decodeStrict(data)
	if err != nil {
		return nil, err
	}

	certs := make([]*x509.Certificate, 0, len(blocks))

	for _, block := range blocks {
		cert, err := parseCert(block)
		if err != nil {
			return nil, err
		}

		certs = append(certs, cert)
	}

	return certs, nil
}

// IsSelfSigned returns whether the given certificate is self-signed: its subject matches its issuer and it verifies its
// own signature. Matching names alone isn't enough, a subordinate CA may share its subject with the CA that issued it.
func IsSelfSigned(cert *x509.Certificate) bool {
	return bytes.Equal(cert.RawSubject, cert.RawIssuer) && cert.CheckSignatureFrom(cert) == nil
}

// parseCert returns the certificate in the given PEM block, failing unless it is a valid certificate.
func parseCert(block *pem.Block) (*x509.Certificate, error) {
	if block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("unexpected PEM block %q", block.Type)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	return cert, nil
}
