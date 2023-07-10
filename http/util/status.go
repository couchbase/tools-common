package util

import (
	"net/http"
)

// TemporaryFailureStatusCodes is a slice of temporary status codes which should be retried by default.
var TemporaryFailureStatusCodes = map[int]struct{}{
	// This error response means that the server, while working as a gateway to get a response needed to handle the
	// request, got an invalid response.
	http.StatusBadGateway: {},
	// The request contained valid data and was understood by the server, but the server is refusing action.
	http.StatusForbidden: {},
	// This error response is given when the server is acting as a gateway and cannot get a response in time.
	http.StatusGatewayTimeout: {},
	// The server has encountered a situation it doesn't know how to handle.
	http.StatusInternalServerError: {},
	// The server is not ready to handle the request. Common causes are a server that is down for maintenance or that is
	// overloaded.
	http.StatusServiceUnavailable: {},
	// The user has sent too many requests in a given amount of time ("rate limiting").
	http.StatusTooManyRequests: {},
	// The server has exceeded the bandwidth specified by the server administrator; this is often used by shared hosting
	// providers to limit the bandwidth of customers.
	509: {},
}

// IsTemporaryFailure returns a boolean indicating whether the provided status code represents a temporary error and
// should be retried.
func IsTemporaryFailure(status int) bool {
	_, ok := TemporaryFailureStatusCodes[status]
	return ok
}
