package tls

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
)

// ValidateKeyPair returns an error unless the given cert/key are non-empty, strictly PEM encoded and match each other.
func ValidateKeyPair(cert, key []byte) error {
	err := validateCerts(cert)
	if err != nil {
		return fmt.Errorf("invalid cert/key pair: %w", err)
	}

	blocks, err := decodeStrict(key)
	if err != nil {
		return fmt.Errorf("invalid cert/key pair: invalid key: %w", err)
	}

	if len(blocks) != 1 || !strings.HasSuffix(blocks[0].Type, "PRIVATE KEY") {
		return errors.New("invalid cert/key pair: key must be a single PEM encoded private key")
	}

	_, err = tls.X509KeyPair(cert, key)
	if err != nil {
		return fmt.Errorf("invalid cert/key pair: %w", err)
	}

	return nil
}

// ValidateCA returns an error unless the given CA is non-empty and strictly made of PEM encoded certificates.
func ValidateCA(ca []byte) error {
	err := validateCerts(ca)
	if err != nil {
		return fmt.Errorf("invalid CA: %w", err)
	}

	return nil
}

// validateCerts returns an error unless the given data is strictly made of one or more valid PEM encoded certificates.
func validateCerts(data []byte) error {
	blocks, err := decodeStrict(data)
	if err != nil {
		return err
	}

	for _, block := range blocks {
		err = validateCert(block)
		if err != nil {
			return err
		}
	}

	return nil
}

// validateCert returns an error unless the given PEM block is a valid certificate.
func validateCert(block *pem.Block) error {
	if block.Type != "CERTIFICATE" {
		return fmt.Errorf("unexpected PEM block %q", block.Type)
	}

	_, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	return nil
}

// decodeStrict returns every PEM block in the given data, failing on any malformed block or trailing data.
func decodeStrict(data []byte) ([]*pem.Block, error) {
	var (
		rest   = data
		blocks []*pem.Block
	)

	for {
		block, r := pem.Decode(rest)
		if block == nil {
			break
		}

		blocks, rest = append(blocks, block), r
	}

	if len(blocks) == 0 {
		return nil, errors.New("no PEM data")
	}

	// pem.Decode silently skips malformed blocks and leaves trailing data in rest
	if bytes.Count(data, []byte("-----BEGIN ")) != len(blocks) || len(bytes.TrimSpace(rest)) > 0 {
		return nil, errors.New("malformed PEM data")
	}

	return blocks, nil
}
