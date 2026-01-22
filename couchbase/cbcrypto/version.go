package cbcrypto

// Version is the version of the cbcrypto file format.
type Version = uint8

const (
	// Version0 represents the initial version of the cbcrypto file format.
	Version0 Version = 0
	// Version1 adds support for key derivation (KBKDF and PBKDF2).
	Version1 Version = 1
)

// CurrentVersion is the latest version of the cbcrypto file format which is supported by this package.
const CurrentVersion = Version1
