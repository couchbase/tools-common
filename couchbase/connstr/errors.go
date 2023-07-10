package connstr

import "errors"

var (
	// ErrInvalidConnectionString is returned if the part matching regex results in a <nil> return value i.e. it doesn't
	// match against the input string.
	ErrInvalidConnectionString = errors.New("invalid connection string")

	// ErrNoAddressesParsed is returned when the resulting parsed connection string doesn't contain any addresses.
	ErrNoAddressesParsed = errors.New("parsed connection string contains no addresses")

	// ErrNoAddressesResolved is returned when the resolved connection string doesn't contain any addresses.
	ErrNoAddressesResolved = errors.New("resolved connection string contains no addresses")

	// ErrBadScheme is returned if the user supplied a scheme that's not supported. Currently 'http', 'https',
	// 'couchbase' and 'couchbases' are supported.
	ErrBadScheme = errors.New("bad scheme")

	// ErrBadPort is returned if the parsed port is an invalid 16 bit unsigned integer.
	ErrBadPort = errors.New("bad port")
)
