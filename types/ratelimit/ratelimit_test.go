package ratelimit

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	leeway      = bufInterval / 10
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

func TestRateLimitedReadAt(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)
		b     = make([]byte, 1024)
		r     = bytes.NewReader(b)

		ctx1, cancel1 = context.WithCancel(context.Background())
		RLReaderAt    = NewRateLimitedReaderAt(ctx1, r, limit)

		ctx2, cancel2  = context.WithCancel(context.Background())
		RLReadAtSeeker = NewRateLimitedReadAtSeeker(ctx2, r, limit)
	)

	testReadWriter(t, RLReaderAt.ReadAt, cancel1)
	testReadWriter(t, RLReadAtSeeker.ReadAt, cancel2)
}

func TestRateLimitedRead(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)
		b     = make([]byte, 2048)
		r     = bytes.NewReader(b)
		rc    = NewMockReadCloser(r)

		readerCtx, readerCancel = context.WithCancel(context.Background())
		rlReader                = NewRateLimitedReader(readerCtx, r, limit)

		readAtSeekerCtx, readAtSeekerCancel = context.WithCancel(context.Background())
		rlReadAtSeeker                      = NewRateLimitedReadAtSeeker(readAtSeekerCtx, r, limit)

		readCloserCtx, readCloserCancel = context.WithCancel(context.Background())
		rlReadCloser                    = NewRateLimitedReadCloser(readCloserCtx, rc, limit)

		readSeekerCtx, readSeekerCancel = context.WithCancel(context.Background())
		rlReadSeeker                    = NewRateLimitedReadSeeker(readSeekerCtx, r, limit)
	)

	testReadWriter(t, func(p []byte, off int64) (int, error) { return rlReader.Read(p) }, readerCancel)
	testReadWriter(t, func(p []byte, off int64) (int, error) { return rlReadAtSeeker.Read(p) }, readAtSeekerCancel)
	testReadWriter(t, func(p []byte, off int64) (int, error) { return rlReadCloser.Read(p) }, readCloserCancel)
	testReadWriter(t, func(p []byte, off int64) (int, error) { return rlReadSeeker.Read(p) }, readSeekerCancel)
}

func TestRateLimitedWrite(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)
		b     = make([]byte, 1024)

		w             = bytes.NewBuffer(b)
		ctx1, cancel1 = context.WithCancel(context.Background())
		RLWriter      = NewRateLimitedWriter(ctx1, w, limit)

		ow              = NewMockWriteAtSeeker(aws.NewWriteAtBuffer(b), 0)
		ctx2, cancel2   = context.WithCancel(context.Background())
		RLWriteAtSeeker = NewRateLimitedWriteAtSeeker(ctx2, ow, limit)
	)

	testReadWriter(t, func(p []byte, off int64) (int, error) { return RLWriter.Write(p) }, cancel1)
	testReadWriter(t, func(p []byte, off int64) (int, error) { return RLWriteAtSeeker.Write(p) }, cancel2)
}

func TestRateLimitedWriteAt(t *testing.T) {
	var (
		limit = rate.NewLimiter(rate.Every(interval), bufSize)
		b     = make([]byte, 1024)

		w             = aws.NewWriteAtBuffer(b)
		ctx1, cancel1 = context.WithCancel(context.Background())
		RLWriterAt    = NewRateLimitedWriterAt(ctx1, w, limit)

		ow              = NewMockWriteAtSeeker(w, 0)
		ctx2, cancel2   = context.WithCancel(context.Background())
		RLWriteAtSeeker = NewRateLimitedWriteAtSeeker(ctx2, ow, limit)
	)

	testReadWriter(t, RLWriterAt.WriteAt, cancel1)
	testReadWriter(t, RLWriteAtSeeker.WriteAt, cancel2)
}

// MockReadCloser implements io.ReadCloser by accepting an io.Reader and mocking the Close method.
// It should only be used for testing.
type MockReadCloser struct {
	io.Reader
}

// NewMockReadCloser returns a MockReadCloser.
func NewMockReadCloser(r io.Reader) *MockReadCloser {
	return &MockReadCloser{r}
}

// Close is mocked i.e. it doesn't do anything
func (rc *MockReadCloser) Close() error {
	return nil
}

// MockWriteAtSeeker maps writes at offset base to offset base+off in the underlying writer.
type MockWriteAtSeeker struct {
	w    io.WriterAt
	base int64 // the original offset
	off  int64 // the current offset
}

// NewMockWriteAtSeeker returns an OffsetWriter that writes to w starting at offset off.
func NewMockWriteAtSeeker(w io.WriterAt, off int64) *MockWriteAtSeeker {
	return &MockWriteAtSeeker{w, off, off}
}

func (o *MockWriteAtSeeker) Write(p []byte) (n int, err error) {
	n, err = o.w.WriteAt(p, o.off)
	o.off += int64(n)

	return
}

func (o *MockWriteAtSeeker) WriteAt(p []byte, off int64) (n int, err error) {
	off += o.base
	return o.w.WriteAt(p, off)
}

func (o *MockWriteAtSeeker) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}
