package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticGetCredentials(t *testing.T) {
	var (
		expected = Credentials{Username: "username", Password: "password"}
		provider = &Static{Credentials: expected}
	)

	actual, err := provider.GetCredentials("")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestStaticGetUserAgent(t *testing.T) {
	provider := &Static{UserAgent: "agent"}
	require.Equal(t, "agent", provider.GetUserAgent())
}
