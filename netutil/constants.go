package netutil

import "time"

const (
	// defaultDialerTimeout is the default net.Dialer Timeout value for transport of the HTTP client.
	defaultDialerTimeout = 30 * time.Second

	// defaultDialerKeepAlive is the default net.Dialer KeepAlive value for transport of the HTTP client.
	defaultDialerKeepAlive = 30 * time.Second

	// defaultIdleConnTimeout is the default IdleConnTimeout value for transport of the HTTP client.
	defaultIdleConnTimeout = 90 * time.Second

	// defaultExpectContinueTimeout is the default ContinueTimeout value for transport of the HTTP client.
	defaultContinueTimeout = 5 * time.Second

	// defaultResponseHeaderTimeout is the default ResponseHeaderTimeout value for transport of the HTTP client.
	defaultResponseHeaderTimeout = 10 * time.Second

	// defaultTLSHandshakeTimeout is the default TLSHandshakeTimeout value for transport of the HTTP client.
	defaultTLSHandshakeTimeout = 10 * time.Second
)
