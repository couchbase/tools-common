package ratelimit

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

const (
	bufSize = 32
	// We want 32 tokens every 50ms
	bufInterval = 50 * time.Millisecond
	interval    = bufInterval / bufSize
	leeway      = bufInterval / 5
)

func testReadWriter(
	t *testing.T, method func(p []byte, off int64) (int, error), cancel context.CancelFunc,
) {
	buf := make([]byte, bufSize)

	t.Run("InitialCallIsImmediate", func(t *testing.T) {
		start := time.Now()

		n, err := method(buf, 0)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.WithinDuration(t, start, time.Now(), leeway)
	})

	for i := 1; i <= 5; i++ {
		t.Run(fmt.Sprintf("SubsequentCallsAreDelayed%d", i), func(t *testing.T) {
			start := time.Now()

			n, err := method(buf, int64(i*len(buf)))
			require.NoError(t, err)
			require.Equal(t, len(buf), n)
			require.WithinDuration(t, start.Add(bufInterval), time.Now(), leeway)
		})
	}

	t.Run("CanDoMoreThanBurst", func(t *testing.T) {
		var (
			count  = 4
			newBuf = make([]byte, bufSize*count)
			start  = time.Now()
			n, err = method(newBuf, 0)
		)

		require.NoError(t, err)
		require.Equal(t, len(buf)*count, n)
		require.WithinDuration(t, start.Add(bufInterval*time.Duration(count)), time.Now(), leeway)
	})

	t.Run("RespectsContextCancel", func(t *testing.T) {
		go func() {
			time.Sleep(interval / 5)
			cancel()
		}()

		_, err := method(buf, 0)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestRateLimitedReaderAt(t *testing.T) {
	var (
		limit       = rate.NewLimiter(rate.Every(interval), bufSize)
		ctx, cancel = context.WithCancel(context.Background())
		b           = make([]byte, 1024)
		r           = bytes.NewReader(b)
		rlr         = NewRateLimitedReader(ctx, r, limit)
	)

	testReadWriter(t, rlr.ReadAt, cancel)
}

func TestRateLimitedReader(t *testing.T) {
	var (
		limit       = rate.NewLimiter(rate.Every(interval), bufSize)
		ctx, cancel = context.WithCancel(context.Background())
		b           = make([]byte, 1024)
		r           = bytes.NewReader(b)
		rlr         = NewRateLimitedReader(ctx, r, limit)
	)

	testReadWriter(t, func(p []byte, off int64) (int, error) { return rlr.Read(p) }, cancel)
}

func TestRateLimitedWriterAt(t *testing.T) {
	var (
		limit       = rate.NewLimiter(rate.Every(interval), bufSize)
		ctx, cancel = context.WithCancel(context.Background())
		b           = make([]byte, 1024)
		w           = aws.NewWriteAtBuffer(b)
		rlw         = NewRateLimitedWriter(ctx, w, limit)
	)

	testReadWriter(t, rlw.WriteAt, cancel)
}
