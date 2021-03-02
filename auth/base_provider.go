package auth

// baseProvider implements the common functionality between the gocb/gocbcore auth provider interfaces.
type baseProvider struct {
	username string
	password string
	mappings HostMappings
}

// newBaseProvider creates a new base provider which implements the common functionality between gocb/gocbcore.
func newBaseProvider(username, password string) baseProvider {
	return baseProvider{
		username: username,
		password: password,
		mappings: GetHostMappings(),
	}
}

// SupportsTLS returns a boolean indicating whether this provider supports TLS connections.
func (p baseProvider) SupportsTLS() bool {
	return true
}

// SupportsNonTLS returns a boolean indicating whether this provider supports non-TLS connections.
func (p baseProvider) SupportsNonTLS() bool {
	return true
}
