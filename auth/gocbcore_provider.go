package auth

import (
	"crypto/tls"

	"github.com/couchbase/gocbcore/v9"
)

// GocbcoreProvider is a provider which builds upon the base provider by implementing the required functions defined by
// the gocbcore 'AuthProvider' interface.
type GocbcoreProvider struct {
	baseProvider
}

// NewGocbcoreProvider creates a new gocbcore provider using the provided username/password.
func NewGocbcoreProvider(username, password string) *GocbcoreProvider {
	return &GocbcoreProvider{
		newBaseProvider(username, password),
	}
}

// Certificate returns the certificate chain used for this connection.
func (g *GocbcoreProvider) Certificate(req gocbcore.AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

// Credentials returns the username/password required to authenticate against the given host.
func (g *GocbcoreProvider) Credentials(req gocbcore.AuthCredsRequest) ([]gocbcore.UserPassPair, error) {
	username, password := GetCredentials(g.username, g.password, req.Endpoint, g.mappings)
	return []gocbcore.UserPassPair{{Username: username, Password: password}}, nil
}
