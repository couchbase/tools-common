package variable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	netutil "github.com/couchbase/tools-common/http/util"
	"github.com/couchbase/tools-common/types/v2/ptr"
)

func TestGetHTTPTimeouts(t *testing.T) {
	type test struct {
		name             string
		envVarValue      string
		expectedTimeouts netutil.HTTPTimeouts
		err              error
	}

	var (
		defaultDialer                  = ptr.To(time.Duration(1))
		defaultKeepAlive               = ptr.To(time.Duration(2))
		defaultTransportIdleConn       = ptr.To(time.Duration(3))
		defaultTransportContinue       = ptr.To(time.Duration(4))
		defaultTransportResponseHeader = ptr.To(time.Duration(5))
		defaultTransportTLSHandshake   = ptr.To(time.Duration(6))
	)

	defaultTimeouts := netutil.HTTPTimeouts{
		Dialer:                  defaultDialer,
		KeepAlive:               defaultKeepAlive,
		TransportIdleConn:       defaultTransportIdleConn,
		TransportContinue:       defaultTransportContinue,
		TransportResponseHeader: defaultTransportResponseHeader,
		TransportTLSHandshake:   defaultTransportTLSHandshake,
	}

	envVar := "CB_CUSTOMTEST_HTTP_TIMEOUTS"

	tests := []*test{
		{
			name:             "EnvVariableNotSet",
			expectedTimeouts: defaultTimeouts,
		},
		{
			name:        "OneTimeoutSet",
			envVarValue: `{"dialer":"1s"}`,
			expectedTimeouts: netutil.HTTPTimeouts{
				Dialer:                  ptr.To(time.Second),
				KeepAlive:               defaultKeepAlive,
				TransportIdleConn:       defaultTransportIdleConn,
				TransportContinue:       defaultTransportContinue,
				TransportResponseHeader: defaultTransportResponseHeader,
				TransportTLSHandshake:   defaultTransportTLSHandshake,
			},
		},
		{
			name: "AllTimeoutsSet",
			envVarValue: `{"dialer":"1s", "keep_alive":"10s", "transport_idle_conn":"100s", "transport_continue":"1m"` +
				`, "transport_response_header":"10m", "transport_tls_handshake":"100m"}`,
			expectedTimeouts: netutil.HTTPTimeouts{
				Dialer:                  ptr.To(time.Second),
				KeepAlive:               ptr.To(10 * time.Second),
				TransportIdleConn:       ptr.To(100 * time.Second),
				TransportContinue:       ptr.To(time.Minute),
				TransportResponseHeader: ptr.To(10 * time.Minute),
				TransportTLSHandshake:   ptr.To(100 * time.Minute),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.envVarValue != "" {
				t.Setenv(envVar, test.envVarValue)
			}

			timeouts, err := GetHTTPTimeouts(envVar, defaultTimeouts)

			require.Equal(t, test.err, err)
			require.Equal(t, test.expectedTimeouts, timeouts)
		})
	}
}
