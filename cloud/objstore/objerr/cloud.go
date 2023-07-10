package objerr

import "errors"

// ErrNoValidCredentialsFound is returned if we have checked all of the supported authentication methods and could not
// create any credentials.
//
// NOTE: This error specifically means that we couldn't find/create any credentials, if a credential is found and ends
// up being invalid, a different error will be returned.
var ErrNoCredentialsFound = errors.New("could not find credentials for the supported authentication methods")
