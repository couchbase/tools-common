package auth

import (
	"crypto/tls"

	"github.com/couchbase/gocb/v2"
)

// GocbProvider is a provider which builds upon the base provider by implementing the required functions defined by the
// gocb 'Authenticator' interface.
type GocbProvider struct {
	baseProvider
}

// NewGocbProvider creates a new gocb provider using the provided username/password.
func NewGocbProvider(username, password string) *GocbProvider {
	return &GocbProvider{
		newBaseProvider(username, password),
	}
}

// Certificate returns the certificate chain used for this connection.
func (g *GocbProvider) Certificate(req gocb.AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

// Credentials returns the username/password required to authenticate against the given host.
func (g *GocbProvider) Credentials(req gocb.AuthCredsRequest) ([]gocb.UserPassPair, error) {
	username, password := GetCredentials(g.username, g.password, req.Endpoint, g.mappings)
	return []gocb.UserPassPair{{Username: username, Password: password}}, nil
}
