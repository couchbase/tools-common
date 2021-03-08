package system

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTotalMemory(t *testing.T) {
	total, err := TotalMemory()
	require.NoError(t, err)
	require.NotZero(t, total)
}
