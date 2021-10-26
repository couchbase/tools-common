package elutil

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/cbrest"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	cluster := cbrest.NewTestCluster(t, cbrest.TestClusterOptions{})
	defer cluster.Close()

	service, err := NewService(ServiceOptions{
		ManagementPort: cluster.Port(),
		Provider:       &aprov.Static{},
	})
	require.NoError(t, err)
	require.NotNil(t, service)
	require.NotNil(t, service.pool)
	require.NotNil(t, service.client)
}

func TestServiceReport(t *testing.T) {
	var actual Event

	handlers := make(cbrest.TestHandlers)
	handlers.Add(http.MethodPost, string(EndpointPostEvent),
		cbrest.NewTestHandlerWithValue(t, http.StatusOK, nil, &actual))

	cluster := cbrest.NewTestCluster(t, cbrest.TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	service, err := NewService(ServiceOptions{
		ManagementPort: cluster.Port(),
		Provider:       &aprov.Static{},
	})
	require.NoError(t, err)

	service.Report(Event{
		EventID: 42,
	})

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, Event{EventID: 42}, actual)
}

func TestServiceReportTooBig(t *testing.T) {
	var actual Event

	handlers := make(cbrest.TestHandlers)
	handlers.Add(http.MethodPost, string(EndpointPostEvent),
		cbrest.NewTestHandlerWithValue(t, http.StatusOK, nil, &actual))

	cluster := cbrest.NewTestCluster(t, cbrest.TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	service, err := NewService(ServiceOptions{
		ManagementPort: cluster.Port(),
		Provider:       &aprov.Static{},
	})
	require.NoError(t, err)

	err = service.report(Event{
		EventID:         42,
		ExtraAttributes: map[string]interface{}{"key": strings.Repeat("value", 650)},
	})
	require.ErrorIs(t, err, ErrTooLarge)
}

// Smoke test to ensure that the 'Report' function is asynchronous.
func TestServiceReportIsAsync(t *testing.T) {
	var (
		gr       string
		handlers = make(cbrest.TestHandlers)
	)

	handlers.Add(http.MethodPost, string(EndpointPostEvent), func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(250 * time.Millisecond)
		gr = "2"
		w.WriteHeader(http.StatusOK)
	})

	cluster := cbrest.NewTestCluster(t, cbrest.TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	service, err := NewService(ServiceOptions{
		ManagementPort: cluster.Port(),
		Provider:       &aprov.Static{},
	})
	require.NoError(t, err)

	service.Report(Event{})

	gr = "1"

	time.Sleep(500 * time.Millisecond)
	require.Equal(t, "2", gr)
}
