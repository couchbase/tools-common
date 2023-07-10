package system

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	version, err := Version()
	require.NoError(t, err)
	require.NotZero(t, version)
}
