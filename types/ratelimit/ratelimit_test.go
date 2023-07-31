package ratelimit

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

const (
	bufSize = 32
	// We want 32 tokens every 50ms
	bufInterval = 50 * time.Millisecond
	interval    = bufInterval / bufSize
	leeway      = bufInterval / 10
)

type discard struct{}

var (
	_ io.Reader   = (*discard)(nil)
	_ io.ReaderAt = (*discard)(nil)
	_ io.Writer   = (*discard)(nil)
	_ io.WriterAt = (*discard)(nil)
	_ io.Seeker   = (*discard)(nil)
	_ io.Closer   = (*discard)(nil)
)

func (d *discard) Read(p []byte) (int, error) {
	return len(p), nil
}

func (d *discard) ReadAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

func (d *discard) Write(p []byte) (int, error) {
	return len(p), nil
}

func (d *discard) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

func (d *discard) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (d *discard) Close() error {
	return nil
}

func testReadWriter(t *testing.T, method func(p []byte, off int64) (int, error), cancel context.CancelFunc) {
	buf := make([]byte, bufSize)

	t.Run("InitialCallIsImmediate", func(t *testing.T) {
		start := time.Now()

		n, err := method(buf, 0)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Greater(t, time.Now(), start)
	})

	for i := 1; i <= 5; i++ {
		t.Run(fmt.Sprintf("SubsequentCallsAreDelayed%d", i), func(t *testing.T) {
			start := time.Now()

			n, err := method(buf, int64(i*len(buf)))
			require.NoError(t, err)
			require.Equal(t, len(buf), n)
			require.Greater(t, time.Now(), start.Add(bufInterval-leeway))
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
		require.Greater(t, time.Now(), start.Add(bufInterval*time.Duration(count)-leeway))
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
		limit = rate.NewLimiter(rate.Every(interval), bufSize)

		ctx1, cancel1 = context.WithCancel(context.Background())
		rlReaderAt    = NewRateLimitedReaderAt(ctx1, &discard{}, limit)

		ctx2, cancel2  = context.WithCancel(context.Background())
		rlReadAtSeeker = NewRateLimitedReadAtSeeker(ctx2, &discard{}, limit)
	)

	testReadWriter(t, rlReaderAt.ReadAt, cancel1)
	testReadWriter(t, rlReadAtSeeker.ReadAt, cancel2)
}

func TestRateLimitedReader(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)

		readerCtx, readerCancel = context.WithCancel(context.Background())
		rlReader                = NewRateLimitedReader(readerCtx, &discard{}, limit)

		readAtSeekerCtx, readAtSeekerCancel = context.WithCancel(context.Background())
		rlReadAtSeeker                      = NewRateLimitedReadAtSeeker(readAtSeekerCtx, &discard{}, limit)

		readCloserCtx, readCloserCancel = context.WithCancel(context.Background())
		rlReadCloser                    = NewRateLimitedReadCloser(readCloserCtx, &discard{}, limit)

		readSeekerCtx, readSeekerCancel = context.WithCancel(context.Background())
		rlReadSeeker                    = NewRateLimitedReadSeeker(readSeekerCtx, &discard{}, limit)
	)

	testReadWriter(t, func(p []byte, _ int64) (int, error) { return rlReader.Read(p) }, readerCancel)
	testReadWriter(t, func(p []byte, _ int64) (int, error) { return rlReadAtSeeker.Read(p) }, readAtSeekerCancel)
	testReadWriter(t, func(p []byte, _ int64) (int, error) { return rlReadCloser.Read(p) }, readCloserCancel)
	testReadWriter(t, func(p []byte, _ int64) (int, error) { return rlReadSeeker.Read(p) }, readSeekerCancel)
}

func TestRateLimitedWrite(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)

		ctx1, cancel1 = context.WithCancel(context.Background())
		rlWriter      = NewRateLimitedWriter(ctx1, &discard{}, limit)

		ctx2, cancel2   = context.WithCancel(context.Background())
		rlWriteAtSeeker = NewRateLimitedWriteAtSeeker(ctx2, &discard{}, limit)
	)

	testReadWriter(t, func(p []byte, _ int64) (int, error) { return rlWriter.Write(p) }, cancel1)
	testReadWriter(t, func(p []byte, _ int64) (int, error) { return rlWriteAtSeeker.Write(p) }, cancel2)
}

func TestRateLimitedWriteAt(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)

		ctx1, cancel1 = context.WithCancel(context.Background())
		rlWriterAt    = NewRateLimitedWriterAt(ctx1, &discard{}, limit)

		ctx2, cancel2   = context.WithCancel(context.Background())
		rlWriteAtSeeker = NewRateLimitedWriteAtSeeker(ctx2, &discard{}, limit)
	)

	testReadWriter(t, rlWriterAt.WriteAt, cancel1)
	testReadWriter(t, rlWriteAtSeeker.WriteAt, cancel2)
}
