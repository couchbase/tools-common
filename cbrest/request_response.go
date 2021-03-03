package cbrest

import (
	"net/url"
)

// Method is a readability wrapper around the method for a given REST request; only the methods defined in the 'http'
// module should be used.
type Method string

// Header is a readability wrapper around key/value pairs which will be set in the REST request header.
type Header map[string]string

// ContentType - Convenience wrapper around the content type of a request. Currently only JSON/form encoded are
// supported.
type ContentType string

const (
	// ContentTypeURLEncoded - Indicates that the body of this request is url encoded.
	ContentTypeURLEncoded ContentType = "application/x-www-form-urlencoded"

	// ContentTypeJSON - Indicates that the body of this request is json encoded.
	ContentTypeJSON ContentType = "application/json"
)

// Request encapsulates the parameters/options which are required when sending a REST request.
type Request struct {
	// Method is the method used for this REST request. Should be one of the constants defined in the 'http' module.
	Method Method

	// Header is additional key/value pairs which will be set in the REST request header.
	Header Header

	// ContentType indicates what type of value we are sending to the cluster node.
	ContentType ContentType

	// Body is the request body itself. This attribute is not always required.
	Body []byte

	// Endpoint is the REST endpoint to hit, all endpoints should be of type 'Endpoint' so that urls are correctly
	// escaped.
	Endpoint Endpoint

	// QueryParameters are additional values which will be encoded and postfixed to the request URL.
	QueryParameters url.Values

	// Service indicates that this request should be sent to a node which is running a specific service.
	Service Service

	// ExpectedStatusCode indicates that when this REST request is successful, we will get this specific status code.
	ExpectedStatusCode int

	// RetryOnStatusCodes is a list of status codes which will be used to indicate that we should retry the request.
	RetryOnStatusCodes []int
}

// Response represents a REST response from the Couchbase Cluster.
type Response struct {
	StatusCode int
	Body       []byte
}
