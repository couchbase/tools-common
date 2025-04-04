package util

import (
	"fmt"
	"net/url"
)

// HostsToConnectionString creates a connection string using the provided hosts. The returned connection string can be
// used when creating a new REST client or when connecting to a cluster via gocbcore.
func HostsToConnectionString(hosts []string) string {
	if len(hosts) == 0 {
		return ""
	}

	connectionString := hosts[0]
	for i := 1; i < len(hosts); i++ {
		connectionString += "," + TrimSchema(hosts[i])
	}

	return connectionString
}

// ReplaceLocalhost uses 'replacement' if the URL 'host' uses localhost as its hostname, retaining host's scheme, port,
// path and query.
func ReplaceLocalhost(host, replacement string) (string, error) {
	parsed, err := url.Parse(host)
	if err != nil {
		return "", fmt.Errorf("could not parse host: %w", err)
	}

	if parsed.Hostname() != "localhost" {
		return host, nil
	}

	port := parsed.Port()
	if port != "" {
		port = ":" + port
	}

	query := ""
	if parsed.RawQuery != "" {
		query = "?" + parsed.RawQuery
	}

	return fmt.Sprintf("%s://%s%s%s%s", parsed.Scheme, replacement, port, parsed.Path, query), nil
}
