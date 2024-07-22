package util

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/cbauth"
)

var (
	ErrUnauthorised         = errors.New("unauthorised")
	ErrForbidden            = errors.New("forbidden")
	ErrCouldNotAuthenticate = errors.New("could not authenticate user")
)

type cbAuthoriser interface {
	AuthWebCreds(r *http.Request) (cbauth.Creds, error)
}

func AuthMiddlewareHandler(
	noAuth bool, permission, userFriendlyName string, w http.ResponseWriter, r *http.Request,
) error {
	if cbauth.Default == nil {
		return cbauth.ErrNotInitialized
	}

	return authMiddlewareHandler(cbauth.Default, noAuth, permission, userFriendlyName, w, r)
}

func authMiddlewareHandler(
	authoriser cbAuthoriser, noAuth bool, permission, userFriendlyName string, w http.ResponseWriter, r *http.Request,
) error {
	if noAuth {
		return nil
	}

	cred, err := authoriser.AuthWebCreds(r)
	// for some reason cbauth does not return the ErrNoAuth when no credentials are provided, it only does when
	// the provided credentials are invalid so we have to manually check for the no credentials find error
	if err == cbauth.ErrNoAuth || (err != nil && err.Error() == "no web credentials found in request") {
		cbauth.SendUnauthorized(w)
		return ErrUnauthorised
	}

	if err != nil {
		HandleErrorWithExtras(ErrorResponse{
			Status: http.StatusInternalServerError,
			Msg:    "Could not authenticate user",
			Extras: err.Error(),
		}, w, nil)

		return ErrCouldNotAuthenticate
	}

	canAccess, err := cred.IsAllowed(permission)
	if err != nil {
		HandleErrorWithExtras(ErrorResponse{
			Status: http.StatusInternalServerError,
			Msg:    "Could not authorize user",
			Extras: err.Error(),
		}, w, nil)

		return ErrCouldNotAuthenticate
	}

	if !canAccess {
		if err := cbauth.SendForbidden(w, userFriendlyName); err != nil {
			return fmt.Errorf("could not send response: %w", ErrForbidden)
		}

		return ErrForbidden
	}

	return nil
}
