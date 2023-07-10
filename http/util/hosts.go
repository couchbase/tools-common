package util

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
