package util

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/types/v2/ptr"
)

func TestHTTPTimeoutsUnmarshalJSON(t *testing.T) {
	type test struct {
		name     string
		input    string
		expected HTTPTimeouts
	}

	tests := []*test{
		{
			name:     "Empty",
			input:    `{}`,
			expected: HTTPTimeouts{},
		},
		{
			name:     "Dialer",
			input:    `{"dialer":"5s"}`,
			expected: HTTPTimeouts{Dialer: ptr.To(5 * time.Second)},
		},
		{
			name:     "KeepAlive",
			input:    `{"keep_alive":"5s"}`,
			expected: HTTPTimeouts{KeepAlive: ptr.To(5 * time.Second)},
		},
		{
			name:     "TransportContinue",
			input:    `{"transport_continue":"5s"}`,
			expected: HTTPTimeouts{TransportContinue: ptr.To(5 * time.Second)},
		},
		{
			name:     "TransportTLSHandshake",
			input:    `{"transport_tls_handshake":"5s"}`,
			expected: HTTPTimeouts{TransportTLSHandshake: ptr.To(5 * time.Second)},
		},
		{
			name:     "TransportResponseHeader",
			input:    `{"transport_response_header":"5s"}`,
			expected: HTTPTimeouts{TransportResponseHeader: ptr.To(5 * time.Second)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actual *HTTPTimeouts

			require.NoError(t, json.Unmarshal([]byte(test.input), &actual), "Expected to be able to marshal value")
			require.Equal(t, *actual, test.expected)
		})
	}
}
