package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticGetCredentials(t *testing.T) {
	provider := &Static{Username: "username", Password: "password"}
	username, password := provider.GetCredentials("")
	require.Equal(t, "username", username)
	require.Equal(t, "password", password)
	require.Zero(t, provider.GetUserAgent())
}

func TestStaticGetUserAgent(t *testing.T) {
	provider := &Static{UserAgent: "agent"}
	username, password := provider.GetCredentials("")
	require.Zero(t, username)
	require.Zero(t, password)
	require.Equal(t, "agent", provider.GetUserAgent())
}
