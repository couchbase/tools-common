package restutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ErrLogFn is a callback function used to log errors occurred in the functions of this package.
type ErrLogFn func(err error)

// ErrorResponse is a struct that defines a basic structure for REST errors. Status should be the HTTPCode for the
// response. The Msg should be a user friendly error that explains what occurs. Extras can be left empty. If included it
// should contain more in depth details about the error.
type ErrorResponse struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
	Extras string `json:"extras,omitempty"`
}

// HandleErrorWithExtras marshals and sends the ErrorResponse as JSON. If an error log function is provided it will be
// called if there are issues marshaling or writing the data to the response writer.
func HandleErrorWithExtras(errRes ErrorResponse, w http.ResponseWriter, errLog ErrLogFn) {
	bytes, err := json.Marshal(&errRes)
	// this will most likely never happen
	if err != nil && errLog != nil {
		errLog(fmt.Errorf("could not marshal error response: %w", err))
	}

	SendJSONResponse(errRes.Status, bytes, w, nil)
}

// SendJSONResponse sends a response with the given status code with the body "data" and content type application/json.
// If the error log function is given it will be used if it encounters an error while writing the response.
func SendJSONResponse(status int, data []byte, w http.ResponseWriter, errLog ErrLogFn) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err := w.Write(data)

	if err != nil && errLog != nil {
		errLog(fmt.Errorf("could not write response: %v", err))
	}
}

// MarshalAndSend will try and marshal the given interface, if it fails it will write an ErrorResponse to the client
// with status 500 and a message saying that the response could not be marshaled. Otherwise it will send the marshaled
// content.
func MarshalAndSend(status int, data any, w http.ResponseWriter, errLog ErrLogFn) {
	var (
		rawData []byte
		err     error
	)

	if data != nil {
		rawData, err = json.Marshal(data)
		if err != nil {
			HandleErrorWithExtras(ErrorResponse{
				Status: http.StatusInternalServerError,
				Msg:    "could not marshal response",
				Extras: err.Error(),
			}, w, errLog)

			return
		}
	}

	SendJSONResponse(status, rawData, w, errLog)
}

// DecodeJSONRequestBody tries to decode the body as JSON. If it fails it will send a 400 response and return false.
// Otherwise it will return true and decode the body into dest.
func DecodeJSONRequestBody(body io.ReadCloser, dest any, w http.ResponseWriter) bool {
	defer body.Close()

	if err := json.NewDecoder(body).Decode(dest); err != nil {
		HandleErrorWithExtras(ErrorResponse{
			Status: http.StatusBadRequest,
			Msg:    "invalid request body",
			Extras: err.Error(),
		}, w, nil)

		return false
	}

	return true
}
