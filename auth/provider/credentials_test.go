package provider

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalCredentials(t *testing.T) {
	credentials := Credentials{
		Username: "username",
		Password: "password",
	}

	data, err := json.Marshal(credentials)
	require.NoError(t, err)

	require.Equal(t, []byte("{}"), data)
}
