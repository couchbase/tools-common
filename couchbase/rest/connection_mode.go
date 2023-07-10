package rest

// SupportedConnectionModes is a slice of the supported connection modes, this should be considered as read only.
var SupportedConnectionModes = []ConnectionMode{
	ConnectionModeDefault,
	ConnectionModeThisNodeOnly,
	ConnectionModeLoopback,
}

// ConnectionMode is a connection mode which slightly changes the behavior of the REST client.
type ConnectionMode int

const (
	// ConnectionModeDefault is the standard connection mode, connections use HTTP/HTTPS depending on the given
	// connection string, and REST requests may be dispatched to any node in the cluster.
	ConnectionModeDefault ConnectionMode = iota

	// ConnectionModeThisNodeOnly means REST request will only be sent to the node contained in the given connection
	// string, connections will use HTTP/HTTPS depending on the given given

	// ConnectionModeThisNodeOnly means connections use HTTP/HTTPS depending on the given connection string, and REST
	// requests will only be dispatched to the given node.
	//
	// NOTE: An error will be raised if this connection mode is used where the connection string contains more than one
	// node.
	ConnectionModeThisNodeOnly

	// ConnectionModeLoopback is equivalent to 'ConnectionModeThisNodeOnly with the added requirement that all requests
	// are sent unencrypted, via loopback (127.0.0.1).
	//
	// NOTE: An error will be raised if this connection mode is used where the connection string contains more than one
	// node, or would create a TLS connection.
	ConnectionModeLoopback
)

// ThisNodeOnly returns a boolean indicating whether we're expecting to only communicate with a single node; the one
// provided in the connection string.
func (c ConnectionMode) ThisNodeOnly() bool {
	return c == ConnectionModeThisNodeOnly || c == ConnectionModeLoopback
}

// AllowTLS returns a boolean indicating whether users can supply https/couchbases in the connection string.
func (c ConnectionMode) AllowTLS() bool {
	return c != ConnectionModeLoopback
}
