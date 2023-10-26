// package random provides cryptographic sourced random utility functions.
package random

import (
	"crypto/rand"
	"math/big"
)

// integer represents the basic integer types.
type integer interface {
	~int | ~int16 | ~int32 | ~int64 | ~uint | ~uint16 | ~uint32 | ~uint64
}

// Integer returns an integer in [mn..mx].
func Integer[T integer](mn, mx T) (T, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(mx-mn)+1))
	if err != nil {
		return *new(T), err
	}

	return T(n.Int64() + int64(mn)), nil
}
