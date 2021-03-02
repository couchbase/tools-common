package errutil

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorsContains(t *testing.T) {
	type test struct {
		name     string
		first    error
		second   string
		expected bool
	}

	tests := []*test{
		{
			name:   "NotContains",
			first:  errors.New("not contains"),
			second: "substr",
		},
		{
			name:     "Equal",
			first:    errors.New("substr"),
			second:   "substr",
			expected: true,
		},
		{
			name:     "ContainsSubString",
			first:    errors.New("contains substr"),
			second:   "substr",
			expected: true,
		},
		{
			name:   "NilError",
			second: "substr",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, Contains(test.first, test.second))
		})
	}
}

func TestErrorUnwrap(t *testing.T) {
	type test struct {
		name     string
		input    error
		expected error
	}

	rootError := errors.New("error")

	tests := []*test{
		{
			name: "NilError",
		},
		{
			name:     "NonNilSingleError",
			input:    rootError,
			expected: rootError,
		},
		{
			name:     "SingleLevelWrap",
			input:    fmt.Errorf("wrap: %w", rootError),
			expected: rootError,
		},
		{
			name:     "MultiLevelWrap",
			input:    fmt.Errorf("wrap: %w", fmt.Errorf("wrap: %w", rootError)),
			expected: rootError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.input, test.expected)
		})
	}
}
