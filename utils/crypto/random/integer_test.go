package random

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInteger(t *testing.T) {
	n, err := Integer[int](1, 2)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)
	require.LessOrEqual(t, n, 2)
}
