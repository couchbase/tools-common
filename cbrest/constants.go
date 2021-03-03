package cbrest

import (
	"time"
)

const (
	// DefaultClientTimeout - The timeout for client connection/single operations i.e. this doesn't include retries.
	DefaultClientTimeout = time.Minute

	// DefaultRequestTimeout - The default timeout for REST requests, note that this includes retries.
	DefaultRequestTimeout = time.Minute

	// DefaultRequestRetries - The number of times to attempt a REST request for known failure scenarios. When sending a
	// new request the overall request timeout is not reset, however, the connection/client level timeout is.
	DefaultRequestRetries = 3

	// DefaultPollTimeout - The default timeout for polling operations e.g. waiting for a bucket to report as 'healthy'.
	DefaultPollTimeout = 5 * time.Minute
)
