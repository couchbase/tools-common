package tlsutil

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidPublicPrivateKeyPair is returned if the users provided public/private keys do not match.
	ErrInvalidPublicPrivateKeyPair = errors.New("private key does not match public key")

	// ErrInvalidPasswordInputDataOrKey is a broader version of the 'ParseCertKeyError' which is used as more of a
	// catchall to indicate something is wrong with the provided public/private key.
	ErrInvalidPasswordInputDataOrKey = errors.New("invalid password, input data or an unsupported public/private key " +
		"format/type")

	// ErrPasswordProvidedButUnused is returned if the user has provided a password, but we've got to a point where it
	// would no longer be used.
	ErrPasswordProvidedButUnused = errors.New("a cert/key password has been provided, but isn't used")
)

// ParseCertKeyError is a more useful variation of 'ErrInvalidPasswordInputDataOrKey' which contains hints as to the
// next steps the user may be able to take to resolve the issue.
type ParseCertKeyError struct {
	what     string
	password bool
}

func (p ParseCertKeyError) Error() string {
	var (
		withPassword = "no password provided"
		encPrefix    = ""
	)

	if p.password {
		withPassword = "password provided"
		encPrefix = "un"
	}

	return fmt.Sprintf("failed to parse %s (with %s), perhaps the type/format is invalid, unsupported or %sencrypted",
		p.what, withPassword, encPrefix)
}
