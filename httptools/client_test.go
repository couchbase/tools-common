package httptools

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	username  = "username"
	password  = "password"
	userAgent = "user-agent"
)

// defaultClient returns the default client for testing
func defaultClient() *Client {
	return NewClient(
		http.DefaultClient,
		&aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		log.StdoutLogger{},
		ClientOptions{},
	)
}

func TestNewClient(t *testing.T) {
	type test struct {
		desc     string
		options  ClientOptions
		expected *Client
	}

	tests := []test{
		{
			desc: "Created client with default options",
			expected: &Client{
				client:       http.DefaultClient,
				logger:       log.NewWrappedLogger(log.StdoutLogger{}),
				authProvider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
			},
		},
		{
			desc: "Created client with custom retries number",
			options: ClientOptions{
				RequestRetries: 10,
			},
			expected: &Client{
				client:         http.DefaultClient,
				requestRetries: 10,
				logger:         log.NewWrappedLogger(log.StdoutLogger{}),
				authProvider:   &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
			},
		},
		{
			desc: "Created client with custom retry log level",
			options: ClientOptions{
				ReqResLogLevel: log.LevelInfo,
			},
			expected: &Client{
				client:         http.DefaultClient,
				reqResLogLevel: log.LevelInfo,
				logger:         log.NewWrappedLogger(log.StdoutLogger{}),
				authProvider:   &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
			},
		},
		{
			desc: "Created client with custom retryer",
			options: ClientOptions{
				Retryer: &retry.Retryer{},
			},
			expected: &Client{
				client:       http.DefaultClient,
				logger:       log.NewWrappedLogger(log.StdoutLogger{}),
				authProvider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
				retryer:      &retry.Retryer{},
			},
		},
		{
			desc: "Created client with custom retryer, retry number and retry log level",
			options: ClientOptions{
				ReqResLogLevel: log.LevelInfo,
				RequestRetries: 10,
				Retryer:        &retry.Retryer{},
			},
			expected: &Client{
				client:         http.DefaultClient,
				reqResLogLevel: log.LevelInfo,
				logger:         log.NewWrappedLogger(log.StdoutLogger{}),
				authProvider:   &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
				requestRetries: 10,
				retryer:        &retry.Retryer{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			client := NewClient(
				http.DefaultClient,
				&aprov.Static{Username: username, Password: password, UserAgent: userAgent},
				log.StdoutLogger{},
				tc.options,
			)
			assert.Equal(t, client, tc.expected)
		})
	}
}

func TestClientExecuteNoRetries(t *testing.T) {
	type test struct {
		desc             string
		handlers         func([]byte) TestHandlers
		request          *Request
		expectedResponse *Response
		expectedErr      func(t *testing.T, err error)
	}

	tests := []test{
		{
			desc: "GET request executed successfully",
			handlers: func(body []byte) TestHandlers {
				handlers := make(TestHandlers)
				handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusOK, []byte("body")))
				return handlers
			},
			request: &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
			},
			expectedResponse: &Response{
				StatusCode: http.StatusOK,
				Body:       []byte("body"),
			},
		},
		{
			desc: "POST request successful",
			handlers: func(body []byte) TestHandlers {
				handlers := make(TestHandlers)
				handlers.Add(http.MethodPost, "/test", NewTestHandler(t, http.StatusAccepted, body))
				return handlers
			},
			request: &Request{
				ContentType:        ContentTypeJSON,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusAccepted,
				Method:             http.MethodPost,
				Body:               []byte(`"request":"test"`),
			},
			expectedResponse: &Response{
				StatusCode: http.StatusAccepted,
				Body:       []byte(`"request":"test"`),
			},
		},
		{
			desc: "GET request endpoint not found",
			handlers: func(body []byte) TestHandlers {
				handlers := make(TestHandlers)
				return handlers
			},
			request: &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
			},
			expectedErr: func(t *testing.T, err error) {
				var expected *EndpointNotFoundError
				require.ErrorAs(t, err, &expected)
			},
			expectedResponse: &Response{
				StatusCode: http.StatusNotFound,
				Body:       []byte(""),
			},
		},
		{
			desc: "GET request unauthorized",
			handlers: func(body []byte) TestHandlers {
				handlers := make(TestHandlers)
				handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusUnauthorized, make([]byte, 0)))
				return handlers
			},
			request: &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
			},
			expectedErr: func(t *testing.T, err error) {
				var expected *AuthenticationError
				require.ErrorAs(t, err, &expected)
			},
			expectedResponse: &Response{
				StatusCode: http.StatusUnauthorized,
				Body:       []byte(""),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			handlers := tc.handlers(tc.request.Body)
			server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
			defer server.Close()

			client := defaultClient()

			tc.request.Host = server.URL

			actual, err := client.ExecuteNoRetries(context.Background(), tc.request)
			if tc.expectedErr != nil {
				tc.expectedErr(t, err)
			}
			require.Equal(t, tc.expectedResponse, actual)
		})
	}
}

func TestClientExecute(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusOK, []byte("body")))
	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))

	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
	}

	expected := &Response{
		StatusCode: http.StatusOK,
		Body:       []byte("body"),
	}

	client := defaultClient()

	actual, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestClientExecuteWithSkipRetry(t *testing.T) {
	var (
		attempts int
		handlers = make(TestHandlers)
	)

	handlers.Add(http.MethodGet, "/test", func(writer http.ResponseWriter, request *http.Request) {
		attempts++

		writer.WriteHeader(http.StatusGatewayTimeout)

		_, err := writer.Write([]byte("Hello, World!"))
		require.NoError(t, err)
	})

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))

	defer server.Close()

	request := &Request{
		Host:                 server.URL,
		ContentType:          ContentTypeURLEncoded,
		Endpoint:             "/test",
		ExpectedStatusCode:   http.StatusOK,
		Method:               http.MethodGet,
		NoRetryOnStatusCodes: []int{http.StatusGatewayTimeout},
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
	require.Equal(t, []byte("Hello, World!"), unexpectedStatus.Body)
	require.Equal(t, 1, attempts)
}

func TestClientExecuteWithDefaultRetries(t *testing.T) {
	for status := range netutil.TemporaryFailureStatusCodes {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			handlers := make(TestHandlers)

			handlers.Add(
				http.MethodGet,
				"/test",
				NewTestHandlerWithRetries(t, 2, status, http.StatusOK, "", []byte("body")),
			)

			server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
			defer server.Close()

			request := &Request{
				Host:               server.URL,
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
			}

			expected := &Response{
				StatusCode: http.StatusOK,
				Body:       []byte("body"),
			}

			client := defaultClient()

			actual, err := client.ExecuteWithRetries(context.Background(), request, nil)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func TestClientExecuteWithRetries(t *testing.T) {
	handlers := make(TestHandlers)

	handlers.Add(
		http.MethodGet,
		"/test",
		NewTestHandlerWithRetries(t, 2, http.StatusTooEarly, http.StatusOK, "", []byte("body")),
	)

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		RetryOnStatusCodes: []int{http.StatusTooEarly},
	}

	expected := &Response{
		StatusCode: http.StatusOK,
		Body:       []byte("body"),
	}

	client := defaultClient()

	actual, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestClientExecuteWithRetryAfter(t *testing.T) {
	type test struct {
		name   string
		status int
		after  func() string // We use a function to ensure durations aren't calculated in advance
		waited bool
	}

	tests := []*test{
		{
			name:   "IntegerNumberOfSeconds",
			status: http.StatusServiceUnavailable,
			after:  func() string { return "1" },
			waited: true,
		},
		{
			name:   "IntegerNumberOfSecondsNot503",
			status: http.StatusGatewayTimeout,
			after:  func() string { return "1" },
		},
		{
			name:   "Date",
			status: http.StatusServiceUnavailable,
			after:  func() string { return time.Now().UTC().Add(2 * time.Second).Format(time.RFC1123) },
			waited: true,
		},
		{
			name:   "DateNotUTC",
			status: http.StatusServiceUnavailable,
			after:  func() string { return time.Now().Add(2 * time.Second).Format(time.RFC1123) },
			waited: true,
		},
		{
			name:   "DateNot503",
			status: http.StatusGatewayTimeout,
			after:  func() string { return time.Now().UTC().Add(2 * time.Second).Format(time.RFC1123) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlers := make(TestHandlers)

			handlers.Add(
				http.MethodGet,
				"/test",
				NewTestHandlerWithRetries(t, 1, test.status, http.StatusOK, test.after(), make([]byte, 0)),
			)

			server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
			defer server.Close()

			request := &Request{
				Host:               server.URL,
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
			}

			expected := &Response{
				StatusCode: http.StatusOK,
				Body:       make([]byte, 0),
			}

			client := defaultClient()

			start := time.Now()

			actual, err := client.ExecuteWithRetries(context.Background(), request, nil)
			require.NoError(t, err)
			require.Equal(t, expected, actual)

			require.Equal(t, test.waited, time.Since(start) >= time.Second)
		})
	}
}

func TestClientExecuteStandardError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusOK, []byte("body")))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusTeapot, // We will not get this status code, so we should error out
		Method:             http.MethodGet,
	}

	expected := &Response{
		StatusCode: http.StatusOK,
		Body:       []byte("body"),
	}

	client := defaultClient()

	actual, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)
	require.Equal(t, expected, actual)

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
}

func TestClientExecuteWithNonIdepotentRequest(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodPost, "/test", NewTestHandler(t, http.StatusTooEarly, make([]byte, 0)))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodPost,
		RetryOnStatusCodes: []int{http.StatusTooEarly},
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var (
		retriesExhausted *RetriesExhaustedError
		unexpectedStatus *UnexpectedStatusCodeError
	)

	require.False(t, errors.As(err, &retriesExhausted)) // testify doesn't appear to have a 'NotErrorAs' function...
	require.ErrorAs(t, err, &unexpectedStatus)
}

func TestClientExecuteWithRetriesExhausted(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusTooEarly, make([]byte, 0)))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		RetryOnStatusCodes: []int{http.StatusTooEarly},
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var retriesExhausted *RetriesExhaustedError

	require.ErrorAs(t, err, &retriesExhausted)
}

func TestClientExecuteAuthError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusUnauthorized, make([]byte, 0)))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var unauthorized *AuthenticationError

	require.ErrorAs(t, err, &unauthorized)
}

func TestClientExecuteInternalServerError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusInternalServerError, []byte("response body")))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var internalServerError *InternalServerError

	require.ErrorAs(t, err, &internalServerError)
	require.Equal(t, []byte("response body"), internalServerError.Body)
}

func TestClientExecute404Status(t *testing.T) {
	handlers := make(TestHandlers)

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var endpointNotFound *EndpointNotFoundError

	require.ErrorAs(t, err, &endpointNotFound)
}

func TestClientExecuteUnexpectedEOF(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithEOF(t))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var unexpectedEOB *UnexpectedEndOfBodyError

	require.ErrorAs(t, err, &unexpectedEOB)
}

func TestClientExecuteSocketClosedInFlight(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithHijack(t))

	server := httptest.NewServer(http.HandlerFunc(handlers.Handle))
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
	}

	client := defaultClient()

	_, err := client.ExecuteWithRetries(context.Background(), request, nil)
	require.Error(t, err)

	var socketClosedInFlight *SocketClosedInFlightError

	require.ErrorAs(t, err, &socketClosedInFlight)
}
