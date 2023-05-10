package httptools

import (
	"time"
)

const (
	// DefaultClientTimeout is the timeout for client connection/single operations i.e. this doesn't include retries.
	DefaultClientTimeout = time.Minute

	// DefaultRequestTimeout is the default timeout for REST requests, note that this includes retries.
	DefaultRequestTimeout = time.Minute

	// DefaultDialerTimeout is the default net.Dialer Timeout value for transport of the HTTP client.
	DefaultDialerTimeout = 30 * time.Second

	// DefaultDialerKeepAlive is the default net.Dialer KeepAlive value for transport of the HTTP client.
	DefaultDialerKeepAlive = 30 * time.Second

	// DefaultTransportIdleConnTimeout is the default IdleConnTimeout value for transport of the HTTP client.
	DefaultTransportIdleConnTimeout = 90 * time.Second

	// DefaultTransportContinueTimeout is the default ContinueTimeout value for transport of the HTTP client.
	DefaultTransportContinueTimeout = 0 * time.Second

	// DefaultResponseHeaderTimeout is the default ResponseHeaderTimeout value for transport of the HTTP client.
	DefaultResponseHeaderTimeout = 0 * time.Second

	// DefaultTLSHandshakeTimeout is the default TLSHandshakeTimeout value for transport of the HTTP client.
	DefaultTLSHandshakeTimeout = 10 * time.Second

	// DefaultRequestRetries is the number of times to attempt a REST request for known failure scenarios. When sending
	// a new request the overall request timeout is not reset, however, the connection/client level timeout is.
	DefaultRequestRetries = 3
)
