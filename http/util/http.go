// Package util provides network related utility functions.
package util

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// NewHTTPTransport returns a new HTTP transport using the given TLS config and timeouts.
func NewHTTPTransport(tlsConfig *tls.Config, timeouts HTTPTimeouts) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   timeoutOrDefault(timeouts.Dialer, defaultDialerTimeout),
		KeepAlive: timeoutOrDefault(timeouts.KeepAlive, defaultDialerKeepAlive),
	}

	return &http.Transport{
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		TLSClientConfig:       tlsConfig,
		DialContext:           dialer.DialContext,
		IdleConnTimeout:       timeoutOrDefault(timeouts.TransportIdleConn, defaultIdleConnTimeout),
		ExpectContinueTimeout: timeoutOrDefault(timeouts.TransportContinue, defaultContinueTimeout),
		ResponseHeaderTimeout: timeoutOrDefault(timeouts.TransportResponseHeader, defaultResponseHeaderTimeout),
		TLSHandshakeTimeout:   timeoutOrDefault(timeouts.TransportTLSHandshake, defaultTLSHandshakeTimeout),
	}
}

// timeoutOrDefault returns the given timeout if it's not nil, otherwise it returns the given default value.
func timeoutOrDefault(timeout *time.Duration, defaultTimeout time.Duration) time.Duration {
	if timeout != nil {
		return *timeout
	}

	return defaultTimeout
}
