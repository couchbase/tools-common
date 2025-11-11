package audit

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"

	cbaudit "github.com/couchbase/goutils/go-cbaudit"
)

type ContextKey string

// ConnContextKey is the key to attach the TCP connection to in a context.
const ConnContextKey ContextKey = "conn"

// HTTPEvent is a struct intended to be used as an audit event with 'go-cbaudit'. It contains fields for the local and
// remote addresses.
type HTTPEvent struct {
	cbaudit.GenericFields
	LocalRemoteIPs

	URL string `json:"url"`
}

type IPAndPort struct {
	IP   string `json:"ip"`
	Port uint16 `json:"port"`
}

// LocalRemoteIPs contains local and remote addresses for an HTTP request.
type LocalRemoteIPs struct {
	Remote *IPAndPort `json:"remote"`
	Local  *IPAndPort `json:"local"`
}

// NewHTTPEvent creates a 'HTTPEvent' with its fields populated from 'req'.
//
// NOTE: The request's context must have the connection as a value with the key 'ConnContextKey', otherwise an error
// is returned.
func NewHTTPEvent(req *http.Request) (*HTTPEvent, error) {
	event := &HTTPEvent{GenericFields: cbaudit.GetAuditBasicFields(req)}

	ips, err := getLocalAndRemoteIPs(req)
	if err != nil {
		return nil, fmt.Errorf("could not get IPs for event: %w", err)
	}

	event.Remote = ips.Remote
	event.Local = ips.Local
	event.URL = req.URL.Path

	return event, nil
}

// getPortAndIP splits 'hostPort' into IP and port, returning an error mentioning 'hostType' if it cannot.
func getPortAndIP(hostPort, hostType string) (string, uint16, error) {
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", 0, fmt.Errorf("could not get address host and port for '%s': %w", hostType, err)
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		slog.Error("(Audit) could not get port using 0 instead", "port", port, "err", err, "hostType",
			hostType)
	}

	return host, uint16(port), nil
}

// getLocalAndRemoteIPs returns a 'LocalRemoteIPs' with its addresses populated from 'r'.
func getLocalAndRemoteIPs(r *http.Request) (*LocalRemoteIPs, error) {
	conn, ok := r.Context().Value(ConnContextKey).(net.Conn)
	if !ok || conn == nil {
		return nil, fmt.Errorf("expected a valid connection stored as a value in the context")
	}

	remoteHost, remotePort, err := getPortAndIP(conn.RemoteAddr().String(), "remote")
	if err != nil {
		return nil, fmt.Errorf("could not get remote address host and port: %w", err)
	}

	localHost, localPort, err := getPortAndIP(conn.LocalAddr().String(), "local")
	if err != nil {
		return nil, fmt.Errorf("could not get local address host and port: %w", err)
	}

	return &LocalRemoteIPs{
		Remote: &IPAndPort{
			IP:   remoteHost,
			Port: remotePort,
		},
		Local: &IPAndPort{
			IP:   localHost,
			Port: localPort,
		},
	}, nil
}
