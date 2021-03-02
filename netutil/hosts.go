package netutil

// HostsToConnectionString creates a connection string using the provided hosts. The returned connection string can be
// used when creating a new REST client or when connecting to a cluster via gocbcore.
func HostsToConnectionString(hosts []string) string {
	var connectionString string

	for index, host := range hosts {
		if index == 0 {
			connectionString = host
		} else {
			connectionString += "," + TrimSchema(host)
		}
	}

	return connectionString
}
