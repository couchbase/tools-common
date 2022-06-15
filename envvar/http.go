package envvar

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/ptrutil"
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

	ptrutil.SetPtrIfNil(&timeouts.Dialer, defaults.Dialer)
	ptrutil.SetPtrIfNil(&timeouts.KeepAlive, defaults.KeepAlive)
	ptrutil.SetPtrIfNil(&timeouts.TransportIdleConn, defaults.TransportIdleConn)
	ptrutil.SetPtrIfNil(&timeouts.TransportContinue, defaults.TransportContinue)
	ptrutil.SetPtrIfNil(&timeouts.TransportResponseHeader, defaults.TransportResponseHeader)
	ptrutil.SetPtrIfNil(&timeouts.TransportTLSHandshake, defaults.TransportTLSHandshake)

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
