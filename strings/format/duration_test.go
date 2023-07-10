package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	type test struct {
		input    time.Duration
		expected string
	}

	tests := []*test{
		{
			input:    time.Second + 500*time.Millisecond,
			expected: "1.5s",
		},
		{
			input:    time.Minute + time.Second + 500*time.Millisecond,
			expected: "1m1s",
		},
	}

	for _, test := range tests {
		t.Run(test.input.String(), func(t *testing.T) {
			actual := Duration(test.input)
			require.Equal(t, test.expected, actual)
		})
	}
}
