package netutil

import "strings"

// TrimSchema trims known schema prefixes from the given host.
func TrimSchema(host string) string {
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "couchbase://")
	host = strings.TrimPrefix(host, "couchbases://")

	return host
}

// ToCouchbaseSchema converts the schema prefix for the given host from http/https to couchbase/couchbases.
func ToCouchbaseSchema(host string) string {
	host = strings.Replace(host, "http://", "couchbase://", 1)
	host = strings.Replace(host, "https://", "couchbases://", 1)

	return host
}
