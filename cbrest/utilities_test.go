package cbrest

import (
	"fmt"
	"testing"

	"github.com/couchbase/tools-common/netutil"
	"github.com/stretchr/testify/require"
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
			err:      &UnknownAuthorityError{},
			expected: true,
		},
		{
			name:     "WrappedError",
			err:      fmt.Errorf("%w", &UnknownAuthorityError{}),
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
			require.Equal(t, test.expected, shouldRetry(test.err))
		})
	}
}
