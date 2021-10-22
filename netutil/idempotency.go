package netutil

import "net/http"

// IsMethodIdempotent returns a boolean indicating whether the given method is idempotent.
func IsMethodIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}
