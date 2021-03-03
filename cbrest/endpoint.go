package cbrest

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

const (
	// EndpointPools - Root of the 'ns_server' REST API. Used to fetch version information from the cluster.
	EndpointPools Endpoint = "/pools"

	// EndpointPoolsDefault - Represents the default cluster (i.e. 'self'). Used for extracting information about the
	// cluster itself.
	EndpointPoolsDefault Endpoint = "/pools/default"

	// EndpointNodesServices - Used during the bootstrapping process to fetch a list of all the nodes in the cluster.
	EndpointNodesServices Endpoint = "/pools/default/nodeServices"
)

// Format returns a new endpoint using 'fmt.Sprintf' to fill in any missing/required elements of the endpoint using the
// given arguments. All arguments will automatically be path escaped before being inserted into the endpoint.
//
// NOTE: No validation takes place to ensure the correct number of arguments are supplied, that's down to you...
func (e Endpoint) Format(args ...string) Endpoint {
	escaped := make([]interface{}, len(args))
	for index, arg := range args {
		escaped[index] = url.PathEscape(arg)
	}

	return Endpoint(fmt.Sprintf(string(e), escaped...))
}
