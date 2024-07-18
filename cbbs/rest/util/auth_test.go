package util

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchbase/cbauth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type creds struct {
	isAllowed bool
	err       error
}

func (*creds) Domain() string      { return "" }
func (*creds) Name() string        { return "" }
func (*creds) User() (_, _ string) { return "", "" }

func (c *creds) IsAllowed(_ string) (bool, error) {
	return c.isAllowed, c.err
}

var _ cbauth.Creds = &creds{}

type authoriser struct {
	creds *creds
	err   error
}

func (a *authoriser) AuthWebCreds(_ *http.Request) (cbauth.Creds, error) {
	return a.creds, a.err
}

var _ cbAuthoriser = &authoriser{}

func TestAuthMiddlewareHandler(t *testing.T) {
	tests := []struct {
		name   string
		noAuth bool

		creds   *creds
		authErr error

		resultErr  error
		statusCode int
	}{
		{
			name:    "UnauthorisedButNoAuth",
			noAuth:  true,
			authErr: cbauth.ErrNoAuth,
		},
		{
			name:       "Unauthorised",
			authErr:    cbauth.ErrNoAuth,
			resultErr:  ErrUnauthorised,
			statusCode: http.StatusUnauthorized,
		},
		{
			name:       "NoCreds",
			authErr:    errors.New("no web credentials found in request"),
			resultErr:  ErrUnauthorised,
			statusCode: http.StatusUnauthorized,
		},
		{
			name:       "AuthWebCredsErr",
			authErr:    assert.AnError,
			resultErr:  ErrCouldNotAuthenticate,
			statusCode: http.StatusInternalServerError,
		},
		{
			name:       "NotAllowed",
			creds:      &creds{},
			resultErr:  ErrForbidden,
			statusCode: http.StatusForbidden,
		},
		{
			name:       "AllowedError",
			creds:      &creds{err: assert.AnError},
			resultErr:  ErrCouldNotAuthenticate,
			statusCode: http.StatusInternalServerError,
		},
		{
			name:  "Success",
			creds: &creds{isAllowed: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authoriser := &authoriser{
				creds: test.creds,
				err:   test.authErr,
			}

			w := httptest.NewRecorder()

			err := authMiddlewareHandler(authoriser, test.noAuth, "", w, nil)
			if test.resultErr == nil {
				require.NoError(t, err)
				return
			}

			require.ErrorIs(t, test.resultErr, err)
			require.Equal(t, test.statusCode, w.Code)
		})
	}
}
