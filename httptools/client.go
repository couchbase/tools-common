package httptools

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/errutil"
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/maths"
	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/retry"
	"golang.org/x/exp/slices"
)

// Client is a generalized client for sending and receiving http requests that wraps various functionality such as error
// handling, logging as well as robust and customizable request retrying.
type Client struct {
	client         *http.Client
	reqResLogLevel log.Level
	logger         log.WrappedLogger
	requestRetries int
	authProvider   aprov.Provider

	retryer *retry.Retryer
}

// ClientOptions wraps all optional parameters for client creation.
type ClientOptions struct {
	// RequestRetries is the number of times a request should be retried.
	// Default is 3.
	RequestRetries int

	// ReqResLogLevel is the level at which each failed request retry should be
	// logged at.
	// Default is TRACE.
	ReqResLogLevel log.Level

	// Retryer is used to overwrite the default retryer if there is a need for it. This means any retryer related
	// parameters in the request object will be ignored and only this retryer will be used as is.
	//
	// The default retryer configuration is:
	// - MaxRetries: 3 or as defined in RequestRetries
	// - MinDelay: 50ms
	// - MaxDelay: 2.5s
	// - ShouldRetry: a function that retries requests on temporary error codes
	//   and the codes specified in the Request struct
	Retryer *retry.Retryer
}

// NewClient creates a new generic REST client.
//
// Parameters:
//   - client: client is the base http client that should be used to send/receive requests.
//   - authProvider: authProvider is the authentication provider object that return the credentials required to send a
//     request to an endpoint.
//   - logger: logger is the passed Logger struct that implements the Log method for logger the user wants to use.
//   - options: options is an object that contains optional parameters for the client.
func NewClient(client *http.Client, authProvider aprov.Provider, logger log.Logger, options ClientOptions) *Client {
	loggerWrapped := log.NewWrappedLogger(logger)

	return &Client{
		client:         client,
		reqResLogLevel: options.ReqResLogLevel,
		requestRetries: options.RequestRetries,
		authProvider:   authProvider,
		retryer:        options.Retryer,
		logger:         loggerWrapped,
	}
}

// RequestRetries returns the number of times a request will be retried for known failure cases.
func (c *Client) RequestRetries() int {
	return c.requestRetries
}

// GetBaseHttpClient returns the http.Client that the client object uses. It only returns a read only copy of the
// client, not a pointer to the actual client.
func (c *Client) GetBaseHTTPClient() http.Client {
	return *c.client
}

// ExecuteWithRetries the given request to completion, using the provided context, reading the entire response body
// whilst honoring request level retries/timeout.
func (c *Client) ExecuteWithRetries(
	ctx context.Context,
	request *Request,
	customizer RetryCustomizer,
) (*Response, error) {
	resp, err := c.Do(ctx, request, customizer) //nolint:bodyclose
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	defer c.CleanupResp(resp)

	response := &Response{StatusCode: resp.StatusCode}

	response.Body, err = ReadBody(request.Method, request.Endpoint, resp.Body, resp.ContentLength)
	if err != nil {
		return response, fmt.Errorf("failed to read response body: %w", err)
	}

	if response.StatusCode == request.ExpectedStatusCode {
		return response, nil
	}

	return response, HandleResponseError(request.Method, request.Endpoint, response.StatusCode, response.Body)
}

// ExecuteNoRetries the given request to completion, using the provided context, reading the entire response body
// without any retry logic. It does check for an expected status code and returns an error if an unexpected code is
// returned.
func (c *Client) ExecuteNoRetries(ctx context.Context, request *Request) (*Response, error) {
	resp, err := c.buildAndDo(retry.NewContext(ctx), request, nil) //nolint:bodyclose
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	defer c.CleanupResp(resp)

	response := &Response{StatusCode: resp.StatusCode}

	response.Body, err = ReadBody(request.Method, request.Endpoint, resp.Body, resp.ContentLength)
	if err != nil {
		return response, fmt.Errorf("failed to read response body: %w", err)
	}

	if response.StatusCode == request.ExpectedStatusCode {
		return response, nil
	}

	return response, HandleResponseError(request.Method, request.Endpoint, response.StatusCode, response.Body)
}

// Do converts and executes the provided request returning the raw HTTP response. In general users should prefer to use
// the 'Execute' function which handles closing resources and returns more informative errors.
//
// NOTE: If the returned error is nil, the Response will contain a non-nil Body which the caller is expected to close.
func (c *Client) Do(ctx context.Context, request *Request, customizer RetryCustomizer) (*http.Response, error) {
	if customizer == nil {
		customizer = &DefaultRetryCustomizer{Request: *request}
	}

	retryer := c.retryer
	if retryer == nil {
		retryer = c.newDefaultRetryer(request, customizer)
	}

	payload, err := retryer.DoWithContext(
		ctx,
		func(ctx *retry.Context) (any, error) { return c.buildAndDo(ctx, request, customizer) }, //nolint:bodyclose
	)

	resp := payload.(*http.Response)

	if err == nil || (resp != nil && resp.StatusCode == request.ExpectedStatusCode) {
		return resp, err
	}

	// The request failed, meaning the response won't be returned to the user, ensure it's cleaned up
	defer c.CleanupResp(resp)

	// Retries exhausted, convert the error into something more informative
	if retry.IsRetriesExhausted(err) {
		err = &RetriesExhaustedError{retries: c.requestRetries, err: enhanceError(errors.Unwrap(err), request, resp)}
	}

	return nil, err
}

// newDefaultRetryer given a specific request and customizer creates a default retryer that respects the parameters in
// the request and has additional logic from the customizer.
func (c *Client) newDefaultRetryer(request *Request, customizer RetryCustomizer) *retry.Retryer {
	shouldRetry := func(ctx *retry.Context, payload any, err error) bool {
		if resp, ok := payload.(*http.Response); ok && resp != nil {
			return c.shouldRetryWithResponse(ctx, request, resp, customizer)
		}

		return c.shouldRetryWithError(ctx, request, err, customizer)
	}

	logRetry := func(ctx *retry.Context, payload any, err error) {
		msg := fmt.Sprintf("(REST) (Attempt %d) (%s) Retrying request to endpoint '%s'", ctx.Attempt(), request.Method,
			request.Endpoint)

		if err != nil {
			msg = fmt.Sprintf("%s: which failed due to error: %s", msg, err)
		} else {
			msg = fmt.Sprintf("%s: which failed with status code %d", msg, payload.(*http.Response).StatusCode)
		}

		// We don't log at error level because we expect some requests to fail and be explicitly handled by the caller.
		c.logger.Warnf(msg)
	}

	cleanup := func(payload any) {
		resp, ok := payload.(*http.Response)
		if !ok || resp == nil {
			return
		}

		c.CleanupResp(resp)
	}

	retryer := retry.NewRetryer(retry.RetryerOptions{
		MaxRetries:  c.requestRetries,
		ShouldRetry: shouldRetry,
		Log:         logRetry,
		Cleanup:     cleanup,
	})

	return &retryer
}

// buildAndDo is a convenience which prepares then performs the provided request.
func (c *Client) buildAndDo(ctx *retry.Context, request *Request, customizer RetryCustomizer) (*http.Response, error) {
	if customizer == nil {
		customizer = &DefaultRetryCustomizer{Request: *request}
	}

	prep, err := c.prepare(ctx, request, customizer)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	resp, err := c.perform(ctx, prep, c.reqResLogLevel, request.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	return resp, nil
}

// Prepare converts the request into a raw HTTP request which can be dispatched to the cluster. Uses the same context
// meaning the request timeout is not reset by retries.
func (c *Client) prepare(ctx *retry.Context, request *Request, customizer RetryCustomizer) (*http.Request, error) {
	host, err := customizer.GetRequestHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, string(request.Method), host+string(request.Endpoint),
		bytes.NewReader(request.Body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// If we received one or more non-nil query parameters ensure that they will be postfixed to the request URL.
	if len(request.QueryParameters) != 0 {
		req.URL.RawQuery = request.QueryParameters.Encode()
	}

	// Using 'Set' overwrites an existing values set in the header, set these values first to that the settings below
	// take precedence.
	for key, value := range request.Header {
		req.Header.Set(key, value)
	}

	req = SetAuthHeaders(*req, host, c.authProvider)

	// Set the content type for the request body. Note that we don't default to a value e.g. if must be set for every
	// request otherwise the string zero value will be used.
	req.Header.Set("Content-Type", string(request.ContentType))

	return req, nil
}

// Perform synchronously executes the provided request returning the response and any error that occurred during the
// process.
func (c *Client) perform(ctx *retry.Context, req *http.Request, level log.Level,
	timeout time.Duration,
) (*http.Response, error) {
	c.logger.Log(level, "(REST) (Attempt %d) (%s) Dispatching request to '%s'", ctx.Attempt(), req.Method, req.URL)

	client := c.client

	// We only use the custom timeout if it is bigger than the client one. This is so that it can be overridden via
	// environmental variables.
	if timeout == -1 || timeout > client.Timeout {
		client = NewHTTPClient(maths.Max(0, timeout), client.Transport)
	}

	resp, err := client.Do(req)
	if err == nil {
		c.logger.Log(level, "(REST) (Attempt %d) (%s) (%d) Received response from '%s'", ctx.Attempt(),
			req.Method, resp.StatusCode, req.URL)

		return resp, nil
	}

	c.logger.Errorf("(REST) (Attempt %d) (%s) Failed to perform request to '%s': %s", ctx.Attempt(), req.Method,
		req.URL, err)

	return nil, HandleRequestError(req, err)
}

// shouldRetryWithError returns a boolean indicating whether the given error is retryable.
func (c *Client) shouldRetryWithError(
	ctx *retry.Context,
	request *Request,
	err error,
	customizer RetryCustomizer,
) bool {
	c.logger.Warnf("(REST) (Attempt %d) (%s) Request to endpoint '%s' failed due to error: %s", ctx.Attempt(),
		request.Method, request.Endpoint, err)

	return customizer.RetryWithErrorExtension(ctx, ShouldRetry(err), err)
}

// shouldRetryWithResponse returns a boolean indicating whether the given request is retryable.
// If the response contains a Retry-After field this will block for the duration of Retry-After and then return true.
func (c *Client) shouldRetryWithResponse(
	ctx *retry.Context,
	request *Request,
	resp *http.Response,
	customizer RetryCustomizer,
) bool {
	// We've got our expected status code, don't retry
	if resp.StatusCode == request.ExpectedStatusCode {
		return false
	}

	c.logger.Warnf("(REST) (Attempt %d) (%s) Request to endpoint '%s' failed with status code %d", ctx.Attempt(),
		request.Method, request.Endpoint, resp.StatusCode)

	// Either this request can't be retried, or the user has explicitly stated that they don't want this status code
	// retried, don't retry.
	if !request.IsIdempotent() || slices.Contains(request.NoRetryOnStatusCodes, resp.StatusCode) {
		return false
	}

	var (
		extension = customizer.RetryWithResponseExtension(ctx, false, resp)
		retry     = extension || netutil.IsTemporaryFailure(resp.StatusCode) ||
			slices.Contains(request.RetryOnStatusCodes, resp.StatusCode)
	)

	if !retry {
		return false
	}

	// if we get a Retry-After in the response this will sleep for the amount of time specified in the response
	waitForRetryAfter(resp)

	return true
}

// CleanupResp drains the response body and ensures it's closed.
func (c *Client) CleanupResp(resp *http.Response) {
	if resp == nil {
		return
	}

	defer resp.Body.Close()

	_, err := io.Copy(io.Discard, resp.Body)
	if err == nil ||
		errors.Is(err, http.ErrBodyReadAfterClose) ||
		errutil.Contains(err, "http: read on closed response body") {
		return
	}

	c.logger.Warnf("(REST) Failed to drain response body due to unexpected error: %s", err)
}
