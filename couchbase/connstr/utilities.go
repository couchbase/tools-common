package connstr

// srvPort - Returns the port which should be used when resolving an SRV record. We don't use the port from the record
// itself since by default, it points to the KV port.
func srvPort(scheme string) uint16 {
	if scheme == "couchbases" {
		return DefaultHTTPSPort
	}

	return DefaultHTTPPort
}
