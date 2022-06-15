package netutil

import (
	"encoding/json"
	"time"
)

// HTTPTimeouts encapsulates the timeouts for a HTTP client into an object which can be parsed as an environment
// variable.
type HTTPTimeouts struct {
	Dialer                  *time.Duration
	KeepAlive               *time.Duration
	TransportIdleConn       *time.Duration
	TransportContinue       *time.Duration
	TransportResponseHeader *time.Duration
	TransportTLSHandshake   *time.Duration
}

func (ct *HTTPTimeouts) UnmarshalJSON(data []byte) error {
	type overlay struct {
		Dialer                  string `json:"dialer,omitempty"`
		KeepAlive               string `json:"keep_alive,omitempty"`
		TransportIdleConn       string `json:"transport_idle_conn,omitempty"`
		TransportContinue       string `json:"transport_continue,omitempty"`
		TransportResponseHeader string `json:"transport_response_header,omitempty"`
		TransportTLSHandshake   string `json:"transport_tls_handshake,omitempty"`
	}

	var decoded overlay

	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return err
	}

	parse := func(duration string) (*time.Duration, error) {
		if duration == "" {
			return nil, nil
		}

		parsed, err := time.ParseDuration(duration)
		if err != nil {
			return nil, err
		}

		return &parsed, nil
	}

	ct.Dialer, err = parse(decoded.Dialer)
	if err != nil {
		return err
	}

	ct.KeepAlive, err = parse(decoded.KeepAlive)
	if err != nil {
		return err
	}

	ct.TransportIdleConn, err = parse(decoded.TransportIdleConn)
	if err != nil {
		return err
	}

	ct.TransportContinue, err = parse(decoded.TransportContinue)
	if err != nil {
		return err
	}

	ct.TransportTLSHandshake, err = parse(decoded.TransportTLSHandshake)
	if err != nil {
		return err
	}

	ct.TransportResponseHeader, err = parse(decoded.TransportResponseHeader)
	if err != nil {
		return err
	}

	return nil
}
