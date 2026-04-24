package objcli

import (
	"encoding/json"
	"os"
	"time"
)

// HTTPTimeouts - Encapsulates the timeouts for a HTTP client into an object which can be parsed as environment
// variable.
type HTTPTimeouts struct {
	Client                  time.Duration
	Dialer                  time.Duration
	KeepAlive               time.Duration
	TransportContinue       time.Duration
	TransportTLSHandshake   time.Duration
	TransportResponseHeader time.Duration
}

// NewHTTPTimeouts - Returns the HTTP timeouts that should be used for the HTTP client used by the object store SDK.
func NewHTTPTimeouts() (HTTPTimeouts, error) {
	env, ok := os.LookupEnv("CB_OBJECT_STORE_HTTP_TIMEOUTS")
	if !ok {
		// We use an empty JSON object by default, this will force the unmarshalling to use all the default values
		env = "{}"
	}

	var timeouts HTTPTimeouts
	err := json.Unmarshal([]byte(env), &timeouts)

	return timeouts, err
}

func (h *HTTPTimeouts) UnmarshalJSON(data []byte) error {
	type overlay struct {
		Client                  string `json:"client,omitempty"`
		Dialer                  string `json:"dialer,omitempty"`
		KeepAlive               string `json:"keep_alive,omitempty"`
		TransportContinue       string `json:"transport_continue,omitempty"`
		TransportTLSHandshake   string `json:"transport_tls_handshake,omitempty"`
		TransportResponseHeader string `json:"transport_response_header,omitempty"`
	}

	var decoded overlay

	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return err
	}

	parseOrDefault := func(duration string, defaultTimeout time.Duration) (time.Duration, error) {
		if duration == "" {
			return defaultTimeout, nil
		}

		return time.ParseDuration(duration)
	}

	// NOTE: We use a relativity large overall timeout because this includes reading the response body. During a restore
	// this will equate to reading the entire Rift data store for a given vBucket; given that there are 1024 vBuckets we
	// shouldn't really require longer than 30 minutes to restore a single vBucket.
	h.Client, err = parseOrDefault(decoded.Client, 30*time.Minute)
	if err != nil {
		return err
	}

	// NOTE: Three minutes by default, as stated by the standard library, TCP timeouts are often around three minutes.
	h.Dialer, err = parseOrDefault(decoded.Dialer, 3*time.Minute)
	if err != nil {
		return err
	}

	h.KeepAlive, err = parseOrDefault(decoded.KeepAlive, time.Minute)
	if err != nil {
		return err
	}

	// NOTE: By default, we don't set an 'Expect Continue' timeout, this means the request body will be sent immediately
	// after writing the request headers.
	h.TransportContinue, err = parseOrDefault(decoded.TransportContinue, 0)
	if err != nil {
		return err
	}

	h.TransportTLSHandshake, err = parseOrDefault(decoded.TransportTLSHandshake, time.Minute)
	if err != nil {
		return err
	}

	// NOTE: By default, we don't set a timeout for reading the response headers, we fall-back to the top-level client
	// timeout, see MB-54994 for more information.
	h.TransportResponseHeader, err = parseOrDefault(decoded.TransportResponseHeader, 0)
	if err != nil {
		return err
	}

	return nil
}
