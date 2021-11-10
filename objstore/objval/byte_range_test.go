package objval

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteRangeValid(t *testing.T) {
	type test struct {
		name     string
		br       *ByteRange
		required bool
		valid    bool
	}

	tests := []*test{
		{
			name:     "RequiredNotProvided",
			required: true,
		},
		{
			name:  "NotRequiredNotProvided",
			valid: true,
		},
		{
			name:  "ProvidedValid",
			br:    &ByteRange{Start: 64, End: 128},
			valid: true,
		},
		{
			name: "ProvidedNotValidStartBeforeEnd",
			br:   &ByteRange{Start: 128, End: 64},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.valid, test.br.Valid(test.required) == nil)
		})
	}
}
