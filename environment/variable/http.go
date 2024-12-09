// Package variable exposes utilities for getting configuration from the environment.
package variable

import (
	"encoding/json"
	"fmt"
	"os"

	netutil "github.com/couchbase/tools-common/http/util"
	"github.com/couchbase/tools-common/types/v2/ptr"
)

// GetHTTPTimeouts returns the timeouts that should be used for a HTTP client from the environment or uses provided
// default values.
//
// NOTE: This function does not guarantee that every field of the returned netutil.HTTPTimeouts is going to be non-nil,
// instead this is ensured by netutil.NewHTTPTransport().
func GetHTTPTimeouts(envVar string, defaults netutil.HTTPTimeouts) (netutil.HTTPTimeouts, error) {
	timeouts, err := getHTTPTimeoutsFromEnv(envVar)
	if err != nil {
		return netutil.HTTPTimeouts{}, fmt.Errorf("failed to get timeouts from environment: %w", err)
	}

	ptr.SetIfNil(&timeouts.Dialer, defaults.Dialer)
	ptr.SetIfNil(&timeouts.KeepAlive, defaults.KeepAlive)
	ptr.SetIfNil(&timeouts.TransportIdleConn, defaults.TransportIdleConn)
	ptr.SetIfNil(&timeouts.TransportContinue, defaults.TransportContinue)
	ptr.SetIfNil(&timeouts.TransportResponseHeader, defaults.TransportResponseHeader)
	ptr.SetIfNil(&timeouts.TransportTLSHandshake, defaults.TransportTLSHandshake)

	return timeouts, nil
}

// getHTTPTimeoutsFromEnv returns the timeouts that should be used for a HTTP client from the environment.
func getHTTPTimeoutsFromEnv(envVar string) (netutil.HTTPTimeouts, error) {
	env, ok := os.LookupEnv(envVar)
	if !ok {
		return netutil.HTTPTimeouts{}, nil
	}

	var timeouts *netutil.HTTPTimeouts

	// Unmarshal will update a default timeout value if a value for the corresponding timeout type is provided using the
	// environment variable
	return *timeouts, json.Unmarshal([]byte(env), &timeouts)
}
