package tlsutil

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/couchbase/tools-common/errutil"

	"github.com/youmark/pkcs8"
	"golang.org/x/crypto/pkcs12"
)

// NewTLSConfig creates a new TLS config which can either skip SSL verification or may contain a x509 certificate pool
// with the users provided ca certificate.
func NewTLSConfig(options TLSConfigOptions) (*tls.Config, error) {
	err := options.Validate()
	if err != nil {
		return nil, err
	}

	config := &tls.Config{
		CipherSuites:             options.CipherSuites,
		ClientAuth:               options.ClientAuthType,
		InsecureSkipVerify:       options.NoSSLVerify,
		MinVersion:               options.MinVersion,
		PreferServerCipherSuites: options.PreferServerCipherSuites,
	}

	err = populateClientCert(config, options)
	if err != nil {
		return nil, fmt.Errorf("failed to populate client certificate: %w", err)
	}

	err = populateClientCAs(config, options)
	if err != nil {
		return nil, fmt.Errorf("failed to populate client certificate: %w", err)
	}

	err = populateRootCAs(config, options)
	if err != nil {
		return nil, fmt.Errorf("failed to populate root certificate authorities: %w", err)
	}

	return config, nil
}

// populateClientCert loads the given client certificate/key pair and populates the required attributes of the given TLS
// configuration.
func populateClientCert(config *tls.Config, options TLSConfigOptions) error {
	// All of the formats expect at least one client certificate to be provided, if one hasn't we're not using
	// certificate authentication, exit early.
	if options.ClientCert == nil {
		return nil
	}

	var (
		cert tls.Certificate
		err  error
	)

	cert.Certificate, err = parseCerts(options)
	if err != nil {
		return fmt.Errorf("failed to parse certificates: %w", err)
	}

	// By default, the 'Leaf' attribute is <nil>; this causes the certificate to be parsed for each TLS handshake, to
	// avoid overhead, we parse it up-front.
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("failed to parse leaf certificate: %w", err)
	}

	cert.PrivateKey, err = parseKey(options)
	if err != nil {
		return fmt.Errorf("failed to parse key: %w", err)
	}

	config.Certificates = []tls.Certificate{cert}

	if cert.Leaf == nil || cert.PrivateKey == nil {
		return ErrInvalidPasswordInputDataOrKey
	}

	if !keysMatch(cert.Leaf, cert.PrivateKey) {
		return ErrInvalidPublicPrivateKeyPair
	}

	return nil
}

// parseCerts returns a slice of all the certificates parsed from the provided client ca file.
func parseCerts(options TLSConfigOptions) ([][]byte, error) {
	var (
		blocks []*pem.Block
		err    error
	)

	if len(options.Password) != 0 && options.ClientKey == nil {
		blocks, err = parseEncryptedPKCS12Blocks(options.ClientCert, options.Password)
	} else {
		blocks = parseUnencryptedBlocks(options.ClientCert)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to parse PEM blocks: %w", err)
	}

	certs := make([][]byte, 0, 1)

	for _, block := range blocks {
		if !strings.Contains(block.Type, "CERTIFICATE") {
			continue
		}

		certs = append(certs, block.Bytes)
	}

	if len(certs) > 0 {
		return certs, nil
	}

	return nil, ParseCertKeyError{what: "certificates", password: len(options.Password) != 0}
}

// parseUnencryptedBlocks returns all the valid PEM blocks from the given data.
func parseUnencryptedBlocks(data []byte) []*pem.Block {
	var (
		block  *pem.Block
		blocks = make([]*pem.Block, 0, 1)
	)

	for {
		block, data = pem.Decode(data)
		if block == nil {
			break
		}

		blocks = append(blocks, block)
	}

	return blocks
}

// parseEncryptedPKCS12Blocks returns all the valid PEM blocks from the given PKCS#12 data.
func parseEncryptedPKCS12Blocks(data, password []byte) ([]*pem.Block, error) {
	blocks, err := pkcs12.ToPEM(data, string(password))
	if err == nil {
		return blocks, nil
	}

	return nil, handleKnownPublicPrivateKeyErrors(err)
}

// parseKey returns the private key which should be used for mTLS authentication.
func parseKey(options TLSConfigOptions) (interface{}, error) {
	data := options.ClientKey

	// For PKCS#12 we don't expect a client ca and a client key, they're both stored in the same file
	if data == nil {
		data = options.ClientCert
	}

	key, err := parseEncryptedKey(data, options)
	if err != nil {
		return nil, fmt.Errorf("failed to parse encrypted private key: %w", err)
	}

	if key != nil {
		return key, nil
	}

	if len(options.Password) != 0 {
		return nil, ErrPasswordProvidedButUnused
	}

	block, _ := pem.Decode(data)
	if block != nil && strings.Contains(block.Type, "PRIVATE KEY") {
		return parseUnencryptedPrivateKey(block.Bytes), nil
	}

	return nil, ParseCertKeyError{what: "private key", password: len(options.Password) != 0}
}

// parseEncryptedKey attempts to decrypt, parse and return private key which should be used for mTLS, the key is
// expected to be in either PKCS#8 or PKCS#12 format.
func parseEncryptedKey(data []byte, options TLSConfigOptions) (interface{}, error) {
	if options.ClientKey != nil {
		return parseEncryptedPKCS8Key(data, options.Password)
	}

	blocks, err := parseEncryptedPKCS12Blocks(data, options.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PKCS#12 PEM blocks: %w", err)
	}

	for _, block := range blocks {
		if !strings.Contains(block.Type, "PRIVATE KEY") {
			continue
		}

		return parseUnencryptedPrivateKey(block.Bytes), nil
	}

	return nil, nil
}

// parseEncryptedPKCS8Key attempts to decrypt, parse and return the private key which should be used for mTLS, the key
// is expected to be in PKCS#8 format.
func parseEncryptedPKCS8Key(data, password []byte) (interface{}, error) {
	key, err := pkcs8.ParsePKCS8PrivateKey(data, password)
	if err == nil {
		return key, nil
	}

	return nil, handleKnownPublicPrivateKeyErrors(err)
}

// parseUnencryptedPrivateKey parses and returns the private key which should be used for mTLS, the keys is expected to
// be unencrypted in either PKCS#1, PKCS#8 or EC format.
//
// NOTE: The parsing of the private key in this function is the same as that which takes place in the 'tls' package from
// the standard library.
//
// See https://github.com/golang/go/blob/go1.17/src/crypto/tls/tls.go#L339-L356 for more information.
func parseUnencryptedPrivateKey(data []byte) interface{} {
	if key, err := x509.ParsePKCS1PrivateKey(data); err == nil {
		return key
	}

	if key, err := x509.ParsePKCS8PrivateKey(data); err == nil {
		return key
	}

	if key, err := x509.ParseECPrivateKey(data); err == nil {
		return key
	}

	return nil
}

// populateClientCAs reads the certificates from the given path and populates the required attributes of the given TLS
// configuration.
func populateClientCAs(config *tls.Config, options TLSConfigOptions) error {
	if options.ClientAuthType == tls.NoClientCert || options.ClientCAs == nil {
		return nil
	}

	config.ClientCAs = x509.NewCertPool()

	ok := config.ClientCAs.AppendCertsFromPEM(options.ClientCAs)
	if !ok {
		return ParseCertKeyError{what: "certificates"}
	}

	return nil
}

// populateRootCAs reads the certificates from the given path and populates the required attributes of the given TLS
// configuration.
func populateRootCAs(config *tls.Config, options TLSConfigOptions) error {
	if options.RootCAs == nil || options.NoSSLVerify {
		return nil
	}

	var err error

	// NOTE: The system cert pool isn't available on Windows, if we get an error continue with a new cert pool
	config.RootCAs, err = x509.SystemCertPool()
	if err != nil {
		config.RootCAs = x509.NewCertPool()
	}

	ok := config.RootCAs.AppendCertsFromPEM(options.RootCAs)
	if !ok {
		return ParseCertKeyError{what: "certificates"}
	}

	return nil
}

// keysMatch returns a boolean indicating whether the given public/private keys match, this is the same sanity test
// performed by the standard library.
//
// See https://github.com/golang/go/blob/go1.17/src/crypto/tls/tls.go#L304-L331 for more information.
func keysMatch(cert *x509.Certificate, key crypto.PrivateKey) bool {
	valid := true // Fallback to true for unknown formats, ultimately the TLS handshake will just fail

	switch priv := key.(type) {
	case *rsa.PrivateKey:
		pub, ok := cert.PublicKey.(*rsa.PublicKey)
		valid = ok && priv.N.Cmp(pub.N) == 0
	case *ecdsa.PrivateKey:
		pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
		valid = ok && priv.X.Cmp(pub.X) == 0 && priv.Y.Cmp(pub.Y) == 0
	case ed25519.PrivateKey:
		pub, ok := cert.PublicKey.(ed25519.PublicKey)
		valid = ok && bytes.Equal(pub, priv.Public().(ed25519.PublicKey))
	}

	return valid
}

// handleKnownPublicPrivateKeyErrors returns 'ErrInvalidPasswordInputDataOrKey' if the given error is a known error that
// should be returned to the user; in all other cases, we'll continue trying to parse the public/private key.
func handleKnownPublicPrivateKeyErrors(err error) error {
	if knownPublicPrivateKeyError(err) {
		return ErrInvalidPasswordInputDataOrKey
	}

	return nil
}

// knownPublicPrivateKeyError returns a boolean indicating whether the given error is a known error which should be
// returned to the user.
func knownPublicPrivateKeyError(err error) bool {
	if errors.Is(err, pkcs12.ErrIncorrectPassword) {
		return true
	}

	msgs := []string{
		"pkcs8: incorrect password",
		"unknown private key type",
		"with unknown algorithm",
	}

	for _, msg := range msgs {
		if errutil.Contains(err, msg) {
			return true
		}
	}

	return false
}
