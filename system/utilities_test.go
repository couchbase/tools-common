package system

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatCommandError(t *testing.T) {
	type test struct {
		name     string
		output   []byte
		err      error
		expected string
	}

	tests := []*test{
		{
			name:     "ExecutedSuccessfullyDisplayStderr",
			output:   []byte("this was the error"),
			err:      errors.New("non-zero exit status"),
			expected: "this was the error",
		},
		{
			name:     "NotExecutedSuccessfully",
			err:      errors.New("not found in $PATH"),
			expected: "not found in $PATH",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, formatCommandError(test.output, test.err))
		})
	}
}
