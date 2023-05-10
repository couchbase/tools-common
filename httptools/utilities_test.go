package httptools

import (
	"crypto/x509"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/netutil"
)

func TestShouldRetry(t *testing.T) {
	type test struct {
		name     string
		err      error
		expected bool
	}

	tests := []*test{
		{
			name:     "ParanoidNil",
			expected: false,
		},
		{
			name:     "SocketClosedInFlight",
			err:      &SocketClosedInFlightError{},
			expected: true,
		},
		{
			name:     "UnknownAuthority",
			err:      &x509.UnknownAuthorityError{},
			expected: true,
		},
		{
			name:     "WrappedError",
			err:      fmt.Errorf("%w", &x509.UnknownAuthorityError{}),
			expected: true,
		},
	}

	for _, msg := range netutil.TemporaryErrorMessages {
		tests = append(tests, &test{
			name:     "msg",
			err:      fmt.Errorf("asdf%sasdf", msg),
			expected: true,
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, ShouldRetry(test.err))
		})
	}
}
