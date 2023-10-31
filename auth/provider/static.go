// Package provider exposes types/interfaces which may be used to provide credentials.
package provider

// Static implements the 'Provider' interface and always returns static credentials/information.
type Static struct {
	UserAgent   string
	Credentials Credentials
}

var _ Provider = (*Static)(nil)

func (s *Static) GetUserAgent() string {
	return s.UserAgent
}

func (s *Static) GetCredentials(_ string) (Credentials, error) {
	return s.Credentials, nil
}
