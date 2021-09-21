package tlsutil

import (
	"crypto/tls"
	"fmt"
)

// TLSConfigOptions encapsulates the available options for creating a new TLS config.
type TLSConfigOptions struct {
	ClientCert string
	ClientKey  string
	Password   []byte

	ClientAuthType tls.ClientAuthType
	ClientCAs      string

	ServerCAs   string
	NoSSLVerify bool

	CipherSuites             []uint16
	MinVersion               uint16
	PreferServerCipherSuites bool
}

// Validate returns an error if the given TLS config is invalid for some reason.
func (t *TLSConfigOptions) Validate() error {
	if len(t.Password) != 0 && (t.ClientCert == "" && t.ClientKey == "") {
		return fmt.Errorf("password provided without a client cert/key")
	}

	if t.ClientCert == "" && t.ClientKey != "" {
		return fmt.Errorf("client key provided without a certificate")
	}

	if t.ClientCert != "" && t.ClientKey == "" && len(t.Password) == 0 {
		return fmt.Errorf("client cert/key file provided without a password; expect an encrypted PKCS#12 file")
	}

	return nil
}
