package hofp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/system"
)

func TestOptionsDefaults(t *testing.T) {
	t.Run("Context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := Options{Context: ctx}
		opts.defaults()

		expected := Options{
			Context:          ctx,
			Size:             system.NumCPU(),
			BufferMultiplier: 1,
			LogPrefix:        "(hofp)",
		}

		require.Equal(t, expected, opts)
	})

	t.Run("Size", func(t *testing.T) {
		opts := Options{LogPrefix: "test prefix:"}
		opts.defaults()

		expected := Options{
			Context:          context.Background(),
			Size:             system.NumCPU(),
			BufferMultiplier: 1,
			LogPrefix:        "test prefix:",
		}

		require.Equal(t, expected, opts)
	})

	t.Run("BufferMultiplier", func(t *testing.T) {
		opts := Options{BufferMultiplier: 42}
		opts.defaults()

		expected := Options{
			Context:          context.Background(),
			Size:             system.NumCPU(),
			BufferMultiplier: 42,
			LogPrefix:        "(hofp)",
		}

		require.Equal(t, expected, opts)
	})

	t.Run("LogPrefix", func(t *testing.T) {
		opts := Options{Size: 1}
		opts.defaults()

		expected := Options{
			Context:          context.Background(),
			Size:             1,
			BufferMultiplier: 1,
			LogPrefix:        "(hofp)",
		}

		require.Equal(t, expected, opts)
	})
}
