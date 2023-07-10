package provider

// Provider is an interface which defines basic functions allowing access credentials/information required to
// authenticate against Couchbase.
type Provider interface {
	GetCredentials(host string) (string, string)
	GetUserAgent() string
}
