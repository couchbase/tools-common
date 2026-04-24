package objcli

import (
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestNewHTTPClientTimeouts(t *testing.T) {
	client, err := NewHTTPClient(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 30*time.Minute, client.Timeout)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)

	require.NotNil(t, transport.DialContext)
	require.NotNil(t, transport.Proxy)

	// Can't be compared using 'reflect.DeepEqual'
	transport.DialContext = nil
	transport.Proxy = nil

	expected := &http.Transport{
		ForceAttemptHTTP2:   true,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConns:        100,
		TLSHandshakeTimeout: time.Minute,
	}

	require.Equal(t, expected, transport)
}

func TestNewHTTPClientWithTokenSource(t *testing.T) {
	client, err := NewHTTPClient(nil, oauth2.StaticTokenSource(&oauth2.Token{}))
	require.NoError(t, err)

	// Assert that we've wrapped the transport in an oauth2 transport which handles token fetching/refreshing
	_, ok := client.Transport.(*oauth2.Transport)
	require.True(t, ok)
}

func TestShouldIgnore(t *testing.T) {
	type test struct {
		name             string
		query            string
		include, exclude []*regexp.Regexp
		expected         bool
	}

	tests := []*test{
		{
			name:    "IncludeOnPath",
			query:   "/path/to/object",
			include: []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("/path/to/object"))},
		},
		{
			name:    "IncludeOnBasename",
			query:   "/path/to/object",
			include: []*regexp.Regexp{regexp.MustCompile("^object$")},
		},
		{
			name:     "IncludeNoneMatched",
			query:    "/path/to/object",
			include:  []*regexp.Regexp{},
			expected: true,
		},
		{
			name:     "ExcludeOnPath",
			query:    "/path/to/object",
			exclude:  []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("/path/to/object"))},
			expected: true,
		},
		{
			name:     "ExcludeOnBasename",
			query:    "/path/to/object",
			exclude:  []*regexp.Regexp{regexp.MustCompile("^object$")},
			expected: true,
		},
		{
			name:    "ExcludeNoneMatched",
			query:   "/path/to/object",
			exclude: []*regexp.Regexp{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, ShouldIgnore(test.query, test.include, test.exclude))
		})
	}
}
