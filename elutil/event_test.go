package elutil

import (
	"encoding/json"
	"testing"
	"time"

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

func TestTimestamp(t *testing.T) {
	now := time.Now().UTC()

	time.Sleep(10 * time.Millisecond)

	// By default, should parse and return as UTC
	stamp, err := time.Parse("2006-01-02T15:04:05.000Z", timestamp())
	require.NoError(t, err)
	require.True(t, stamp.After(now))
}

func TestStableISO8601(t *testing.T) {
	iso8601 := stableISO8601(time.Time{})
	require.Contains(t, iso8601, "000Z") // Assert that the trailing zeros aren't being trimmed
}
