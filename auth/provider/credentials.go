package provider

// Credentials represents a username/password pair.
type Credentials struct {
	Username string `json:"-"`
	Password string `json:"-"`
}
