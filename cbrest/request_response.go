package cbrest

import (
	"github.com/couchbase/tools-common/httptools"
	"github.com/couchbase/tools-common/netutil"
)

// Request encapsulates the parameters/options which are required when sending a REST request.
type Request struct {
	httptools.Request

	// Service indicates that this request should be sent to a node which is running a specific service. If 'Host' is
	// provided, this attribute is ignored.
	Service Service
}

// IsIdempotent returns a boolean indicating whether this request is idempotent and may be retried.
func (r *Request) IsIdempotent() bool {
	return r.Idempotent || netutil.IsMethodIdempotent(string(r.Method))
}

// StreamingResponse encapsulates a single streaming response payload/error.
type StreamingResponse struct {
	// Payload is the raw payload received from a streaming endpoint.
	Payload []byte

	// Error is an error received during streaming, after the first error, the stream will be terminated.
	Error error
}
