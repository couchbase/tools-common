package slices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	type Snapshot struct {
		ID string
	}

	snapshots := []Snapshot{
		{
			ID: "a",
		},
		{
			ID: "b",
		},
	}

	ids := Map[[]Snapshot, []string](snapshots, func(snapshot Snapshot) string { return snapshot.ID })

	require.Equal(t, []string{"a", "b"}, ids)
}
