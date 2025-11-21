package provider

// Credentials represents a username/password pair or an auth token.
type Credentials struct {
	Username  string `json:"-"`
	Password  string `json:"-"`
	AuthToken string `json:"-"`
}
