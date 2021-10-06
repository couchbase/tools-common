package hofp

import (
	"testing"

	"github.com/couchbase/tools-common/system"
	"github.com/stretchr/testify/require"
)

func TestOptionsDefaults(t *testing.T) {
	t.Run("Size", func(t *testing.T) {
		opts := Options{LogPrefix: "test prefix:"}
		opts.defaults()
		require.Equal(t, Options{Size: system.NumCPU(), BufferMultiplier: 1, LogPrefix: "test prefix:"}, opts)
	})

	t.Run("BufferMultiplier", func(t *testing.T) {
		opts := Options{BufferMultiplier: 42}
		opts.defaults()
		require.Equal(t, Options{Size: system.NumCPU(), BufferMultiplier: 42, LogPrefix: "(hofp)"}, opts)
	})

	t.Run("LogPrefix", func(t *testing.T) {
		opts := Options{Size: 1}
		opts.defaults()
		require.Equal(t, Options{Size: 1, BufferMultiplier: 1, LogPrefix: "(hofp)"}, opts)
	})
}
