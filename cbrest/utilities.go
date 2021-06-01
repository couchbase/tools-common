package cbrest

import (
	"bufio"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/couchbase/tools-common/netutil"
)

// readBody returns the entire response body returning an informative error in the case where the response body is less
// than the expected length.
func readBody(method Method, endpoint Endpoint, reader io.Reader, contentLength int64) ([]byte, error) {
	body, err := io.ReadAll(bufio.NewReader(reader))
	if err == nil {
		return body, nil
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, &UnexpectedEndOfBodyError{
			method:   method,
			endpoint: endpoint,
			expected: contentLength,
			got:      len(body),
		}
	}

	return nil, err
}

// setAuthHeaders is a utility function which sets all the request headers which are provided by the 'AuthProvider'.
func setAuthHeaders(host string, authProvider *AuthProvider, req *http.Request) {
	// Use the auth provider to populate the credentials
	req.SetBasicAuth(authProvider.provider.GetCredentials(host))

	// Set the 'User-Agent' so that we can trace how these requests are handled by the cluster
	req.Header.Set("User-Agent", authProvider.GetUserAgent())
}

// handleRequestError is a utility function which converts a failed REST request error (hard failure as returned by the
// standard library) into a more useful/user friendly error.
func handleRequestError(req *http.Request, err error) error {
	// If we received and unknown authority error, wrap it with our informative error explaining the alternatives
	// available to the user.
	var unknownAuth x509.UnknownAuthorityError
	if errors.As(err, &unknownAuth) {
		return &UnknownAuthorityError{inner: err}
	}

	// If we receive an EOF error, wrap it with a useful error message containing the method/endpoint
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &SocketClosedInFlightError{method: req.Method, endpoint: req.URL.Path}
	}

	return err
}

// handleResponseError is a utility function which converts a failed REST request (soft failure i.e. the request itself
// was successful) into a more useful/user friendly error.
func handleResponseError(method Method, endpoint Endpoint, statusCode int, body []byte) error {
	switch statusCode {
	case http.StatusForbidden:
		type overlay struct {
			Permissions []string `json:"permissions"`
		}

		var data overlay

		// Purposely ignored as some endpoints may not return the permissions or a body at all. In this case we just set
		// the permissions in the AuthorizationError to nil.
		_ = json.Unmarshal(body, &data)

		return &AuthorizationError{
			method:      method,
			endpoint:    endpoint,
			permissions: data.Permissions,
		}
	case http.StatusUnauthorized:
		return &AuthenticationError{method: method, endpoint: endpoint}
	case http.StatusInternalServerError:
		return &InternalServerError{method: method, endpoint: endpoint, body: body}
	case http.StatusNotFound:
		return &EndpointNotFoundError{method: method, endpoint: endpoint}
	}

	return &UnexpectedStatusCodeError{
		Status:   statusCode,
		method:   method,
		endpoint: endpoint,
		body:     body,
	}
}

// shouldRetry returns a boolean indicating whether the request which returned the given error should be retried.
func shouldRetry(err error) bool {
	var (
		socketClosed *SocketClosedInFlightError
		unknownAuth  *UnknownAuthorityError
	)

	return err != nil && netutil.IsTemporaryError(err) || errors.As(err, &socketClosed) || errors.As(err, &unknownAuth)
}
