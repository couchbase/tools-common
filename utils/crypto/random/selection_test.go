package random

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelection(t *testing.T) {
	const (
		chars = "0123456789abcdefghijklmnopqrstuvwxyz"
		n     = 6
	)

	in := func(t *testing.T, c string) {
		for _, e := range c {
			require.Contains(t, chars, string(e))
		}
	}

	first, err := Selection([]byte(chars), n)
	require.NoError(t, err)
	require.Len(t, first, n)
	in(t, string(first))

	second, err := Selection([]byte(chars), n)
	require.NoError(t, err)
	require.Len(t, second, n)
	in(t, string(second))

	require.NotEqual(t, first, second)
}
