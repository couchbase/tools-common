package elutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventMarshalJSON(t *testing.T) {
	var event Event

	data, err := json.Marshal(event)
	require.NoError(t, err)

	type overlay struct {
		Timestamp string `json:"timestamp"`
		UUID      string `json:"uuid"`
	}

	var decoded overlay

	require.NoError(t, json.Unmarshal(data, &decoded))
	require.NotZero(t, decoded.Timestamp)
	require.NotZero(t, decoded.UUID)
}
