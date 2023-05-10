package httptools

import (
	"net/http"

	"github.com/couchbase/tools-common/retry"
)

// RetryCustomizer defines an interface for injecting custom behaviour into the default httptools retry logic.
type RetryCustomizer interface {
	// RetryWithErrorExtension is called when a request returns any error.
	// Parameters:
	// - ctx: the context of the retry. Contains the attempt number.
	// - shouldRetry: is a boolean that depicts the previous default checks decision. This can be useful when the custom
	//   check is not as important and we want to prioritize the default behaviour.
	// - err: the error that was thrown by the request.
	RetryWithErrorExtension(ctx *retry.Context, shouldRetry bool, err error) bool

	// RetryWithResponseExtension is called when a request returns a any response successfully.
	// Parameters:
	// - ctx: the context of the retry. Contains the attempt number.
	// - shouldRetry: is a boolean that depicts the previous default checks decision. This can be useful when the custom
	//   check is not as important and we want to prioritize the default behaviour.
	// - resp: the http response we received from the request.
	RetryWithResponseExtension(ctx *retry.Context, shouldRetry bool, resp *http.Response) bool

	// GetRequestHost is called when forming a request and returns the host that should be used. Useful when the host
	// name is dynamic and we need some special logic each time we run the request to get the host name.
	GetRequestHost(ctx *retry.Context) (string, error)
}

var _ RetryCustomizer = new(DefaultRetryCustomizer)

// DefaultRetryCustomizer implements the RetryCustomizer interface. It is used when the caller does not set their own
// custom behaviour and just want the default behaviour.
type DefaultRetryCustomizer struct {
	Request
}

// RetryWithErrorExtension returns shouldRetry
func (d *DefaultRetryCustomizer) RetryWithErrorExtension(ctx *retry.Context, shouldRetry bool, err error) bool {
	return shouldRetry
}

// RetryWithResponseExtension returns shouldRetry
func (d *DefaultRetryCustomizer) RetryWithResponseExtension(
	ctx *retry.Context,
	shouldRetry bool,
	resp *http.Response,
) bool {
	return shouldRetry
}

// GetRequestHost returns the host name set in the Request object.
func (d *DefaultRetryCustomizer) GetRequestHost(ctx *retry.Context) (string, error) {
	return d.Request.Host, nil
}
