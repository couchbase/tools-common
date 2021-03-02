package netutil

import (
	"net/http"

	"github.com/couchbase/tools-common/slice"
)

// TemproraryFailureStatusCodes is a slice of temporary status codes which should be retried by default.
var TemproraryFailureStatusCodes = []int{
	http.StatusServiceUnavailable,
}

// IsTemporaryFailure returns a boolean indicating whether the provided status code represents a temporary error and
// should be retried.
func IsTemporaryFailure(status int) bool {
	return slice.ContainsInt(TemproraryFailureStatusCodes, status)
}
