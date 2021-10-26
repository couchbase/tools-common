package elutil

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/cbrest"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cluster := cbrest.NewTestCluster(t, cbrest.TestClusterOptions{})
	defer cluster.Close()

	client, err := NewClient(ServiceOptions{
		ManagementPort: cluster.Port(),
		Provider:       &aprov.Static{},
	})
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientPostEvent(t *testing.T) {
	var actual json.RawMessage

	handlers := make(cbrest.TestHandlers)
	handlers.Add(http.MethodPost, string(EndpointPostEvent),
		cbrest.NewTestHandlerWithValue(t, http.StatusOK, nil, &actual))

	cluster := cbrest.NewTestCluster(t, cbrest.TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	client, err := NewClient(ServiceOptions{
		ManagementPort: cluster.Port(),
		Provider:       &aprov.Static{},
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	require.NoError(t, client.PostEvent([]byte(`{"key":"value"}`)))
	require.Equal(t, json.RawMessage(`{"key":"value"}`), actual)
}
