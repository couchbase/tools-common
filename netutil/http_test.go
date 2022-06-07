package netutil

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewHTTPTransport(t *testing.T) {
	type test struct {
		name                  string
		config                *tls.Config
		timeouts              HTTPTimeouts
		IdleConnTimeout       time.Duration
		ExpectContinueTimeout time.Duration
		ResponseHeaderTimeout time.Duration
		TLSHandshakeTimeout   time.Duration
	}

	var (
		customIdleConnTimeout       = defaultIdleConnTimeout + 1
		customContinueTimeout       = defaultContinueTimeout + 1
		customResponseHeaderTimeout = defaultResponseHeaderTimeout + 1
		customTLSHandshakeTimeout   = defaultTLSHandshakeTimeout + 1
	)

	tests := []*test{
		{
			name:                  "NoTLSConfigNoTimeoutsExpectedDefaults",
			IdleConnTimeout:       defaultIdleConnTimeout,
			ExpectContinueTimeout: defaultContinueTimeout,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		},
		{
			name:                  "WithTLSConfigNoTimeoutsExpectedDefaults",
			config:                &tls.Config{},
			IdleConnTimeout:       defaultIdleConnTimeout,
			ExpectContinueTimeout: defaultContinueTimeout,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		},
		{
			name: "IdleConnTimeout",
			timeouts: HTTPTimeouts{
				TransportIdleConn: &customIdleConnTimeout,
			},
			IdleConnTimeout:       customIdleConnTimeout,
			ExpectContinueTimeout: defaultContinueTimeout,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		},
		{
			name: "ContinueTimeout",
			timeouts: HTTPTimeouts{
				TransportContinue: &customContinueTimeout,
			},
			IdleConnTimeout:       defaultIdleConnTimeout,
			ExpectContinueTimeout: customContinueTimeout,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		},
		{
			name: "ResponseHeaderTimeout",
			timeouts: HTTPTimeouts{
				TransportResponseHeader: &customResponseHeaderTimeout,
			},
			IdleConnTimeout:       defaultIdleConnTimeout,
			ExpectContinueTimeout: defaultContinueTimeout,
			ResponseHeaderTimeout: customResponseHeaderTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		},
		{
			name: "TransportContinueTimeout",
			timeouts: HTTPTimeouts{
				TransportTLSHandshake: &customTLSHandshakeTimeout,
			},
			IdleConnTimeout:       defaultIdleConnTimeout,
			ExpectContinueTimeout: defaultContinueTimeout,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			TLSHandshakeTimeout:   customTLSHandshakeTimeout,
		},
		{
			name: "AllTransportTimeouts",
			timeouts: HTTPTimeouts{
				TransportIdleConn:       &customIdleConnTimeout,
				TransportContinue:       &customContinueTimeout,
				TransportResponseHeader: &customResponseHeaderTimeout,
				TransportTLSHandshake:   &customTLSHandshakeTimeout,
			},
			IdleConnTimeout:       customIdleConnTimeout,
			ExpectContinueTimeout: customContinueTimeout,
			ResponseHeaderTimeout: customResponseHeaderTimeout,
			TLSHandshakeTimeout:   customTLSHandshakeTimeout,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := NewHTTPTransport(test.config, test.timeouts)

			require.Equal(t, true, transport.ForceAttemptHTTP2)
			require.Equal(t, 100, transport.MaxIdleConns)

			require.Equal(t, test.config, transport.TLSClientConfig)
			require.Equal(t, test.IdleConnTimeout, transport.IdleConnTimeout)
			require.Equal(t, test.ExpectContinueTimeout, transport.ExpectContinueTimeout)
			require.Equal(t, test.ResponseHeaderTimeout, transport.ResponseHeaderTimeout)
			require.Equal(t, test.TLSHandshakeTimeout, transport.TLSHandshakeTimeout)
		})
	}
}
