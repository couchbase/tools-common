package cbrest

import (
	"bufio"
	"errors"
	"io"
	"net/http"
)

// readBody returns the entire response body returning an informative error in the case where the response body is less
// than the expected length.
func readBody(request *Request, response *http.Response) ([]byte, error) {
	body, err := io.ReadAll(bufio.NewReader(response.Body))
	if err == nil {
		return body, nil
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, &UnexpectedEndOfBodyError{
			method:   request.Method,
			endpoint: request.Endpoint,
			expected: response.ContentLength,
			got:      len(body),
		}
	}

	return nil, err
}
