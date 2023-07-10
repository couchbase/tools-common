package objerr

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleError(t *testing.T) {
	type test struct {
		name   string
		input  error
		output error
	}

	tests := []*test{
		{
			name:   "ErrEndpointResolutionFailed",
			input:  &net.DNSError{IsNotFound: true},
			output: ErrEndpointResolutionFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, HandleError(test.input), test.output)
			require.ErrorIs(t, TryHandleError(test.input), test.output)
		})
	}
}

func TestHandleErrorUnknown(t *testing.T) {
	require.ErrorIs(t, HandleError(assert.AnError), assert.AnError)
}

func TestTryHandleErrorUnknown(t *testing.T) {
	require.Nil(t, TryHandleError(assert.AnError))
}
