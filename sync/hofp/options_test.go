package hofp

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/utils/v3/system"
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
			Logger:           slog.Default(),
		}

		require.Equal(t, expected, opts)
	})

	t.Run("Size", func(t *testing.T) {
		var opts Options

		opts.defaults()

		expected := Options{
			Context:          context.Background(),
			Size:             system.NumCPU(),
			BufferMultiplier: 1,
			Logger:           slog.Default(),
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
			Logger:           slog.Default(),
		}

		require.Equal(t, expected, opts)
	})

	t.Run("Logger", func(t *testing.T) {
		logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

		opts := Options{Size: 1, Logger: logger}

		opts.defaults()

		expected := Options{
			Context:          context.Background(),
			Size:             1,
			BufferMultiplier: 1,
			Logger:           logger,
		}

		require.Equal(t, expected, opts)
	})
}
