package cbrest

import (
	"fmt"
	"net/http"

	"github.com/couchbase/tools-common/httptools"
	"github.com/couchbase/tools-common/retry"
	"golang.org/x/exp/slices"
)

type CustomRetry struct {
	client  *Client
	request *Request
}

var _ httptools.RetryCustomizer = new(CustomRetry)

// RetryWithErrorExtension returns a boolean indicating whether the given error is retryable.
func (c *CustomRetry) RetryWithErrorExtension(ctx *retry.Context, shouldRetry bool, err error) bool {
	if shouldRetry {
		// We always update the cluster config after a failed request, since some connection failures may be due to an
		// attempt to address a node which is no longer a member of the cluster or even running Couchbase Server. For
		// example, the 'connection refused' error.
		c.client.waitUntilUpdated(ctx)

		return true
	}

	return false
}

// RetryWithResponseExtension returns a boolean indicating whether the given request is retryable.
//
// NOTE: When CCP is enabled, this function may block until the client has the latest available cluster config.
func (c *CustomRetry) RetryWithResponseExtension(ctx *retry.Context, shouldRetry bool, resp *http.Response) bool {
	if slices.Contains([]int{http.StatusUnauthorized}, resp.StatusCode) {
		c.client.waitUntilUpdated(ctx)
		return true
	}

	return false
}

func (c *CustomRetry) GetRequestHost(ctx *retry.Context) (string, error) {
	// Get the fully qualified address to the node that we are sending this request to
	host, err := c.client.serviceHostForRequest(c.request, ctx.Attempt()-1)
	if err != nil {
		return "", fmt.Errorf("failed to get host for service '%s': %w", c.request.Service, err)
	}

	return host, nil
}
