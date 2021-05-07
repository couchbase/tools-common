package netutil

import (
	"errors"
	"net"
	"strings"
)

// TemporaryErrorMessages is a slice of known error messages which may be returned by the Go standard library when
// attempting to perform network operations.
var TemporaryErrorMessages = []string{
	"bad record MAC",                   // src/crypto/tls/alert.go
	"broken pipe",                      // src/syscall/zerrors_linux_amd64.go
	"connection refused",               // src/syscall/zerrors_linux_amd64.go
	"connection reset",                 // src/syscall/zerrors_linux_amd64.go
	"connection timed out",             // src/syscall/zerrors_linux_amd64.go
	"http: ContentLength=",             // src/net/http/transfer.go
	"i/o timeout",                      // src/net/net.go
	"net/http: TLS handshake timeout",  // src/net/http/transport.go
	"server closed idle connection",    // src/net/http/transport.go
	"stream error:",                    // src/net/http/h2_bundle.go
	"transport connection broken",      // src/net/http/transport.go
	"unexpected EOF reading trailer",   // src/net/http/transfer.go
	"use of closed network connection", // src/internal/poll/fd.go
}

// IsTemporaryError returns a boolean indicating whether the provided error is a result of a temporary failure and
// should be retried.
func IsTemporaryError(err error) bool {
	var (
		dnsErr     *net.DNSError
		unknownErr net.UnknownNetworkError
		opError    *net.OpError
	)

	if errors.As(err, &dnsErr) || errors.As(err, &unknownErr) || (errors.As(err, &opError) && opError.Op == "dial") {
		return true
	}

	type temporary interface {
		Temporary() bool
	}

	if t, ok := err.(temporary); ok && t.Temporary() {
		return true
	}

	for _, msg := range TemporaryErrorMessages {
		if strings.Contains(err.Error(), msg) {
			return true
		}
	}

	return false
}
