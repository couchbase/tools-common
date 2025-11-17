package util

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClose(t *testing.T) {
	body := io.NopCloser(strings.NewReader("Hello, World!"))

	err := Close(body)
	require.NoError(t, err)

	rem, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Empty(t, rem)
}
