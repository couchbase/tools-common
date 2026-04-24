package objcli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testutil "github.com/couchbase/tools-common/testing/util"
)

func TestHTTPTimeoutsUnmarshalJSON(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected HTTPTimeouts
	}

	fillDefaults := func(timeouts HTTPTimeouts) HTTPTimeouts {
		if timeouts.Client == 0 {
			timeouts.Client = 30 * time.Minute
		}

		if timeouts.Dialer == 0 {
			timeouts.Dialer = 3 * time.Minute
		}

		if timeouts.KeepAlive == 0 {
			timeouts.KeepAlive = time.Minute
		}

		if timeouts.TransportTLSHandshake == 0 {
			timeouts.TransportTLSHandshake = time.Minute
		}

		return timeouts
	}

	tests := []*test{
		{
			name:     "Empty",
			input:    `{}`,
			expected: fillDefaults(HTTPTimeouts{}),
		},
		{
			name:     "Client",
			input:    `{"client":"5s"}`,
			expected: fillDefaults(HTTPTimeouts{Client: 5 * time.Second}),
		},
		{
			name:     "Dialer",
			input:    `{"dialer":"5s"}`,
			expected: fillDefaults(HTTPTimeouts{Dialer: 5 * time.Second}),
		},
		{
			name:     "KeepAlive",
			input:    `{"keep_alive":"5s"}`,
			expected: fillDefaults(HTTPTimeouts{KeepAlive: 5 * time.Second}),
		},
		{
			name:     "TransportContinue",
			input:    `{"transport_continue":"5s"}`,
			expected: fillDefaults(HTTPTimeouts{TransportContinue: 5 * time.Second}),
		},
		{
			name:     "TransportTLSHandshake",
			input:    `{"transport_tls_handshake":"5s"}`,
			expected: fillDefaults(HTTPTimeouts{TransportTLSHandshake: 5 * time.Second}),
		},
		{
			name:     "TransportResponseHeader",
			input:    `{"transport_response_header":"5s"}`,
			expected: fillDefaults(HTTPTimeouts{TransportResponseHeader: 5 * time.Second}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actual HTTPTimeouts

			testutil.UnmarshalJSON(t, []byte(test.input), &actual)
			require.Equal(t, test.expected, actual)
		})
	}
}
