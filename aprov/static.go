package aprov

// Static implements the 'Provider' interface and always returns static credentials/information.
type Static struct {
	UserAgent, Username, Password string
}

var _ Provider = (*Static)(nil)

func (s *Static) GetCredentials(_ string) (string, string) {
	return s.Username, s.Password
}

func (s *Static) GetUserAgent() string {
	return s.UserAgent
}
