package ratelimit

import (
	"context"
	"fmt"
	"io"

	"github.com/couchbase/tools-common/ioiface"
	"github.com/couchbase/tools-common/maths"

	"golang.org/x/time/rate"
)

// RateLimitedReader will use its limiter as a rate limit on the number of bytes read.
type RateLimitedReader struct {
	ctx     context.Context
	r       ioiface.ReadAtSeeker
	limiter *rate.Limiter
}

// RateLimitedWriter will use its limiter as a rate limit on the number of bytes written.
type RateLimitedWriter struct {
	ctx     context.Context
	w       io.WriterAt
	limiter *rate.Limiter
}

var (
	_ ioiface.ReadAtSeeker = (*RateLimitedReader)(nil)
	_ io.WriterAt          = (*RateLimitedWriter)(nil)
)

// Create a new RateLimitedReader which respects "limiter" in terms of the number of bytes read.
func NewRateLimitedReader(ctx context.Context, r ioiface.ReadAtSeeker, limiter *rate.Limiter) *RateLimitedReader {
	return &RateLimitedReader{ctx: ctx, r: r, limiter: limiter}
}

// Read will read into p whilst respecting the rate limit.
func (r *RateLimitedReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// ReadAt will read into p from off whilst respecting the rate limit.
func (r *RateLimitedReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// Seek sets the offset for the next read.
func (r *RateLimitedReader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

// Create a new RateLimitedWriter which respects "limiter" in terms of the number of bytes written.
func NewRateLimitedWriter(ctx context.Context, w io.WriterAt, limiter *rate.Limiter) *RateLimitedWriter {
	return &RateLimitedWriter{ctx: ctx, w: w, limiter: limiter}
}

// WriteAt will write from p at off whilst respecting the rate limit.
func (w *RateLimitedWriter) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.w.WriteAt(p, off)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(w.ctx, w.limiter, n)
}

// waitChunked waits for n tokens in chunks of the limiter's burst size. This is because rate.Limiter will only allow
// at most its burst number of tokens to be drained at once, so if we want to wait for more than several calls to wait
// are required.
func waitChunked(ctx context.Context, limiter *rate.Limiter, n int) error {
	maxChunkSize := limiter.Burst()

	for n > 0 {
		waitFor := maths.Min(n, maxChunkSize)
		if lErr := limiter.WaitN(ctx, waitFor); lErr != nil {
			return fmt.Errorf("could not wait for limiter: %w", lErr)
		}

		n -= waitFor
	}

	return nil
}
