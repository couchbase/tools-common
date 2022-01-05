package objval

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartEqual(t *testing.T) {
	type test struct {
		name     string
		a, b     Part
		expected bool
	}

	tests := []*test{
		{
			name:     "Empty",
			expected: true,
		},
		{
			name:     "JustIDEqual",
			a:        Part{ID: "42"},
			b:        Part{ID: "42"},
			expected: true,
		},
		{
			name: "JustIDNotEqual",
			a:    Part{ID: "64"},
			b:    Part{ID: "128"},
		},
		{
			name:     "JustNumberEqual",
			a:        Part{Number: 42},
			b:        Part{Number: 42},
			expected: true,
		},
		{
			name: "JustNumberNotEqual",
			a:    Part{Number: 64},
			b:    Part{Number: 128},
		},
		{
			name:     "JustSizeEqual",
			a:        Part{Size: 42},
			b:        Part{Size: 42},
			expected: true,
		},
		{
			name: "JustSizeNotEqual",
			a:    Part{Size: 64},
			b:    Part{Size: 128},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.a.Equal(test.b))
		})
	}
}
