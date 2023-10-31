package provider

// Provider is an interface which defines basic functions allowing access credentials/information required to
// authenticate against Couchbase.
type Provider interface {
	GetUserAgent() string
	GetCredentials(host string) (Credentials, error)
}
