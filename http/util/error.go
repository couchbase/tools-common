package util

import (
	"errors"
	"io"
	"net"
	"strings"
)

// TemporaryErrorMessages is a slice of known error messages which may be returned by the Go standard library when
// attempting to perform network operations.
var TemporaryErrorMessages = []string{
	// src/crypto/tls/alert.go
	"bad record MAC",
	// src/syscall/zerrors_linux_amd64.go
	"broken pipe",
	// src/syscall/zerrors_linux_amd64.go
	"connection refused",
	// src/syscall/zerrors_linux_amd64.go
	"connection reset",
	// src/syscall/zerrors_linux_amd64.go
	"connection timed out",
	// src/net/http/h2_bundle.go
	"http2: client connection force closed via ClientConn.Close",
	// src/net/http/h2_bundle.go
	"http2: timeout awaiting response headers",
	// src/net/http/transfer.go
	"http: ContentLength=",
	// src/net/net.go
	"i/o timeout",
	// src/net/http/transport.go
	"net/http: TLS handshake timeout",
	// src/net/http/transport.go
	"net/http: timeout awaiting response headers",
	// src/net/http/transport.go
	"server closed idle connection",
	// src/net/http/h2_bundle.go
	"stream error:",
	// src/net/http/transport.go
	"transport connection broken",
	// src/net/http/transfer.go
	"unexpected EOF reading trailer",
	// src/internal/poll/fd.go
	"use of closed network connection",
}

// IsTemporaryError returns a boolean indicating whether the provided error is a result of a temporary failure and
// should be retried.
func IsTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	var (
		dnsErr     *net.DNSError
		unknownErr net.UnknownNetworkError
		opError    *net.OpError
	)

	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.As(err, &dnsErr) ||
		errors.As(err, &unknownErr) ||
		(errors.As(err, &opError) && opError.Op == "dial") {
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
