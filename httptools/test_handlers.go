package httptools

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/testutil"
)

// TestHandlers is a readbility wrapper around the endpoint handlers for a test cluster.
type TestHandlers map[string]http.HandlerFunc

// Add a new handler to the endpoint handlers, note that the method is required to ensure unique handlers for each
// endpoint.
func (e TestHandlers) Add(method, endpoint string, handler http.HandlerFunc) {
	e[fmt.Sprintf("%s:%s", method, endpoint)] = handler
}

// Handle utility function which handles the provided request returning a boolen indicating whether a handler was found.
func (e TestHandlers) Handle(writer http.ResponseWriter, request *http.Request) {
	handler, ok := e[fmt.Sprintf("%s:%s", request.Method, request.URL.Path)]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	handler(writer, request)
}

// NewTestHandler creates the most basic type of handler which will respond with the provided status/body.
func NewTestHandler(t *testing.T, status int, body []byte) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(status)

		_, err := writer.Write(body)
		require.NoError(t, err)
	}
}

// NewTestHandlerWithRetries builds upon the basic handler by simulating a flaky/busy endpoint which forces retries a
// configurable number of times before providing a valid response.
func NewTestHandlerWithRetries(t *testing.T, numRetries, retryStatus, successStatus int,
	after string, body []byte,
) http.HandlerFunc {
	var retries int

	return func(writer http.ResponseWriter, request *http.Request) {
		defer func() { retries++ }()

		status := retryStatus
		if retries >= numRetries {
			status = successStatus
		}

		writer.Header().Set("Retry-After", after)
		writer.WriteHeader(status)

		_, err := writer.Write(body)
		require.NoError(t, err)
	}
}

// NewTestHandlerWithEOF creates a handler which will cause an EOF error when attempting to read the body.
func NewTestHandlerWithEOF(t *testing.T) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", "1")

		writer.WriteHeader(http.StatusOK)

		_, err := writer.Write(make([]byte, 0))
		require.NoError(t, err)
	}
}

// NewTestHandlerWithHijack creates a handler which will hijack the connection an immediately close it; this is
// simulating a socket closed in flight error.
func NewTestHandlerWithHijack(t *testing.T) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		hijacker, ok := writer.(http.Hijacker)
		require.True(t, ok)

		conn, _, err := hijacker.Hijack()
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
}

// NewTestHandlerWithValue creates a handler which reads and stores the request body in the provided interface. This
// should be used to validate that a requests body was the expected value.
func NewTestHandlerWithValue(t *testing.T, status int, body []byte, value any) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		switch request.Header.Get("Content-Type") {
		case string(ContentTypeJSON):
			require.NoError(t, json.NewDecoder(request.Body).Decode(&value))
		case string(ContentTypeURLEncoded):
			values, err := url.ParseQuery(string(testutil.ReadAll(t, request.Body)))
			require.NoError(t, err)

			p, ok := value.(*url.Values)
			require.True(t, ok)

			*p = values
		}

		writer.WriteHeader(status)

		_, err := writer.Write(body)
		require.NoError(t, err)
	}
}
