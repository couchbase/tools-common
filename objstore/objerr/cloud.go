package objerr

import "errors"

// ErrNoValidCredentialsFound returned if we've search all the available authentication methods and come up empty.
var ErrNoValidCredentialsFound = errors.New("no valid credentials found")
