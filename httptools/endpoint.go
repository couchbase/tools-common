package httptools

import (
	"fmt"
	"net/url"
)

// Endpoint represents a single REST endpoint. Requests should only be dispatched to endpoints which exist in this file
// i.e. they shouldn't be created on the fly.
//
// NOTE: Endpoints should not include query parameters, they may be supplied as raw 'url.Values' via the 'Request' data
// structure and will be encoded and postfixed to the request URL accordingly.
type Endpoint string

// Format returns a new endpoint using 'fmt.Sprintf' to fill in any missing/required elements of the endpoint using the
// given arguments. All arguments will automatically be path escaped before being inserted into the endpoint.
//
// NOTE: No validation takes place to ensure the correct number of arguments are supplied, that's down to you...
func (e Endpoint) Format(args ...string) Endpoint {
	escaped := make([]any, len(args))
	for index, arg := range args {
		escaped[index] = url.PathEscape(arg)
	}

	return Endpoint(fmt.Sprintf(string(e), escaped...))
}
