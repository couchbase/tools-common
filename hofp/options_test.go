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
		require.Equal(t, Options{Size: system.NumCPU(), LogPrefix: "test prefix:"}, opts)
	})

	t.Run("LogPrefix", func(t *testing.T) {
		opts := Options{Size: 1}
		opts.defaults()
		require.Equal(t, Options{Size: 1, LogPrefix: "(HOFP)"}, opts)
	})
}
