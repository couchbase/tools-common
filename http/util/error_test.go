package util

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTemporaryError(t *testing.T) {
	type test struct {
		name     string
		input    error
		expected bool
	}

	tests := []*test{
		{
			name: "Nil",
		},
		{
			name:  "NotTemporary",
			input: errors.New("not temporary"),
		},
		{
			name:     "DNSError",
			input:    &net.DNSError{},
			expected: true,
		},
		{
			name:     "UnknownNetworkError",
			input:    net.UnknownNetworkError(""),
			expected: true,
		},
		{
			name:     "DialError",
			input:    &net.OpError{Op: "dial", Err: errors.New("does not matter")},
			expected: true,
		},
		{
			name:     "ImplementsTemporaryInterface",
			input:    &net.OpError{Op: "accept", Err: syscall.ECONNRESET},
			expected: true,
		},
		{
			name:     "UnexpectedEOF",
			input:    io.ErrUnexpectedEOF,
			expected: true,
		},
	}

	// Ensure we're adding an expected number of test cases below
	require.Len(t, TemporaryErrorMessages, 15)

	for _, msg := range TemporaryErrorMessages {
		tests = append(tests, &test{
			name:     msg,
			input:    fmt.Errorf("asdf%sasdf", msg),
			expected: true,
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, IsTemporaryError(test.input))
		})
	}
}
