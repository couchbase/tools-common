package objcli

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"path"
	"regexp"
	"time"

	"golang.org/x/oauth2"

	envvar "github.com/couchbase/tools-common/environment/variable"
)

const (
	DefaultMaxRetries      = 5
	DefaultRetryMinBackoff = 250 * time.Millisecond
	DefaultRetryMaxBackoff = 10 * time.Second
)

// NewHTTPClient - Helper function to create a new HTTP client with the provided TLS config and timeouts which can be
// configured using environment variables.
func NewHTTPClient(tlsConfig *tls.Config, tokenSource oauth2.TokenSource) (*http.Client, error) {
	timeouts, err := NewHTTPTimeouts()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP timeout settings: %w", err)
	}

	var transport http.RoundTripper = &http.Transport{
		DialContext:           (&net.Dialer{Timeout: timeouts.Dialer, KeepAlive: timeouts.KeepAlive}).DialContext,
		ExpectContinueTimeout: timeouts.TransportContinue,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		Proxy:                 http.ProxyFromEnvironment,
		ResponseHeaderTimeout: timeouts.TransportResponseHeader,
		TLSClientConfig:       tlsConfig,
		TLSHandshakeTimeout:   timeouts.TransportTLSHandshake,
	}

	if tokenSource != nil {
		transport = &oauth2.Transport{Base: transport, Source: tokenSource}
	}

	return &http.Client{Timeout: timeouts.Client, Transport: transport}, nil
}

// GetMaxRetries - gets the maximum number of retries to do for an object store operation.
//
// NOTE: This does not work for Google Object Storage as it does not expose retries.
func GetMaxRetries() int {
	retries, ok := envvar.GetInt("CB_OBJSTORE_MAX_RETRIES")
	if !ok || retries < 0 {
		retries = DefaultMaxRetries
	} else {
		log.Printf("(Objstore) Max retries changed to: %d\n", retries)
	}

	return retries
}

// GetMaxRetryBackoff - Returns the maximum backoff duration for the object store client.
//
// NOTE: This does not work for Google Object Storage as it does not expose retries.
func GetMaxRetryBackoff() time.Duration {
	maxBackoff, ok := envvar.GetDuration("CB_OBJSTORE_MAX_BACKOFF")
	if !ok || maxBackoff == 0 {
		maxBackoff = DefaultRetryMaxBackoff
	} else {
		log.Printf("(Objstore) Max retry backoff changed to: %s\n", maxBackoff)
	}

	return maxBackoff
}

// ShouldIgnore uses the given regular expressions to determine if we should skip listing the provided file.
func ShouldIgnore(query string, include, exclude []*regexp.Regexp) bool {
	ignore := func(regexes []*regexp.Regexp) bool {
		for _, regex := range regexes {
			if regex.MatchString(query) || regex.MatchString(path.Base(query)) {
				return true
			}
		}

		return false
	}

	return (include != nil && !ignore(include)) || (exclude != nil && ignore(exclude))
}
