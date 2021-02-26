package restutil

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSendJSONResponse(t *testing.T) {
	var (
		errorOccurred error
		statusCode    int
		data          []byte
	)

	errLog := func(err error) {
		errorOccurred = err
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		SendJSONResponse(statusCode, data, w, errLog)
	}))
	defer testServer.Close()

	type testCase struct {
		name       string
		statusCode int
		data       []byte
	}

	cases := []testCase{
		{
			name:       "OKNilData",
			statusCode: http.StatusOK,
		},
		{
			name:       "OKEmptyData",
			statusCode: http.StatusOK,
			data:       []byte{},
		},
		{
			name:       "OKWithBody",
			statusCode: http.StatusOK,
			data:       []byte(`{"something":"something"}`),
		},
		{
			name:       "500WithBody",
			statusCode: http.StatusInternalServerError,
			data:       []byte(`{"something":"something"}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errorOccurred = nil
			statusCode = tc.statusCode
			data = tc.data

			res, err := http.Get(testServer.URL + "/")
			require.NoError(t, err)
			require.NoError(t, errorOccurred)

			defer res.Body.Close()

			require.Equal(t, tc.statusCode, res.StatusCode)
			require.Equal(t, "application/json", res.Header.Get("Content-Type"))
			require.EqualValues(t, len(tc.data), res.ContentLength)

			if len(tc.data) == 0 {
				return
			}

			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			require.Equal(t, tc.data, body)
		})
	}
}

func TestHandleErrorWithExtras(t *testing.T) {
	var (
		errRes        ErrorResponse
		errorOccurred error
	)

	errLog := func(err error) {
		errorOccurred = err
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		HandleErrorWithExtras(errRes, w, errLog)
	}))
	defer testServer.Close()

	type testCase struct {
		name   string
		errRes ErrorResponse
	}

	cases := []testCase{
		{
			name:   "NoExtras",
			errRes: ErrorResponse{Status: http.StatusNotFound, Msg: "not found"},
		},
		{
			name:   "Extras",
			errRes: ErrorResponse{Status: http.StatusNotFound, Msg: "not found", Extras: "we did look"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errorOccurred = nil
			errRes = tc.errRes

			res, err := http.Get(testServer.URL + "/")
			require.NoError(t, err)
			require.NoError(t, errorOccurred)

			defer res.Body.Close()

			require.Equal(t, tc.errRes.Status, res.StatusCode)
			require.Equal(t, "application/json", res.Header.Get("Content-Type"))

			require.NoError(t, err)
			require.NotEqual(t, 0, res.ContentLength)

			var outRes ErrorResponse
			require.NoError(t, json.NewDecoder(res.Body).Decode(&outRes))
			require.Equal(t, tc.errRes, outRes)
		})
	}
}
