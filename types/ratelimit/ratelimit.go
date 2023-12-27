// Package ratelimit exposes rate limited io implementations.
package ratelimit

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/time/rate"

	ioiface "github.com/couchbase/tools-common/types/iface"
)

// RateLimitedReader will use its limiter as a rate limit on the number of bytes read.
type RateLimitedReader struct {
	ctx     context.Context
	r       io.Reader
	limiter *rate.Limiter
}

// NewRateLimitedReader creates a new RateLimitedReader which respects "limiter" in terms of the number of bytes read.
func NewRateLimitedReader(ctx context.Context, r io.Reader, limiter *rate.Limiter) *RateLimitedReader {
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

// RateLimitedReaderAt will use its limiter as a rate limit on the number of bytes read.
type RateLimitedReaderAt struct {
	ctx     context.Context
	r       io.ReaderAt
	limiter *rate.Limiter
}

// NewRateLimitedReaderAt creates a new RateLimitedReaderAt which respects "limiter" in terms of the number of bytes
// read.
func NewRateLimitedReaderAt(ctx context.Context, r io.ReaderAt, limiter *rate.Limiter) *RateLimitedReaderAt {
	return &RateLimitedReaderAt{ctx: ctx, r: r, limiter: limiter}
}

// ReadAt will read into p from off whilst respecting the rate limit.
func (r *RateLimitedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// RateLimitedReadAtSeeker will use its limiter as a rate limit on the number of bytes read.
type RateLimitedReadAtSeeker struct {
	ctx     context.Context
	r       ioiface.ReadAtSeeker
	limiter *rate.Limiter
}

// NewRateLimitedReadAtSeeker creates a new RateLimitedReadAtSeeker which respects "limiter" in terms of the number of
// bytes read.
func NewRateLimitedReadAtSeeker(ctx context.Context, r ioiface.ReadAtSeeker, limiter *rate.Limiter,
) *RateLimitedReadAtSeeker {
	return &RateLimitedReadAtSeeker{ctx: ctx, r: r, limiter: limiter}
}

// Read will read into p whilst respecting the rate limit.
func (r *RateLimitedReadAtSeeker) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// ReadAt will read into p from off whilst respecting the rate limit.
func (r *RateLimitedReadAtSeeker) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// Seek sets the offset for the next read.
func (r *RateLimitedReadAtSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

// RateLimitedReadCloser will use its limiter as a rate limit on the number of bytes read.
type RateLimitedReadCloser struct {
	ctx     context.Context
	r       io.ReadCloser
	limiter *rate.Limiter
}

// NewRateLimitedReadCloser creates a new RateLimitedReadCloser which respects "limiter" in terms of the number of bytes
// read.
func NewRateLimitedReadCloser(ctx context.Context, r io.ReadCloser, limiter *rate.Limiter) *RateLimitedReadCloser {
	return &RateLimitedReadCloser{ctx: ctx, r: r, limiter: limiter}
}

// Read will read into p whilst respecting the rate limit.
func (r *RateLimitedReadCloser) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// Close will close the reader
func (r *RateLimitedReadCloser) Close() error {
	return r.r.Close()
}

// RateLimitedReadSeeker will use its limiter as a rate limit on the number of bytes read.
type RateLimitedReadSeeker struct {
	ctx     context.Context
	r       io.ReadSeeker
	limiter *rate.Limiter
}

// NewRateLimitedReadSeeker creates a RateLimitedReadSeeker which respects "limiter" in terms of the number of bytes
// read.
func NewRateLimitedReadSeeker(ctx context.Context, r io.ReadSeeker, limiter *rate.Limiter) *RateLimitedReadSeeker {
	return &RateLimitedReadSeeker{ctx: ctx, r: r, limiter: limiter}
}

// Read will read into p whilst respecting the rate limit.
func (r *RateLimitedReadSeeker) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(r.ctx, r.limiter, n)
}

// Seek sets the offset for the next read.
func (r *RateLimitedReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

// RateLimitedWriter will use its limiter as a rate limit on the number of bytes written.
type RateLimitedWriter struct {
	ctx     context.Context
	w       io.Writer
	limiter *rate.Limiter
}

// NewRateLimitedWriter creates a new RateLimitedWriter which respects "limiter" in terms of the number of bytes
// written.
func NewRateLimitedWriter(ctx context.Context, w io.Writer, limiter *rate.Limiter) *RateLimitedWriter {
	return &RateLimitedWriter{ctx: ctx, w: w, limiter: limiter}
}

// Write will write from p whilst respecting the rate limit.
func (w *RateLimitedWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(w.ctx, w.limiter, n)
}

// RateLimitedWriterAt will use its limiter as a rate limit on the number of bytes written.
type RateLimitedWriterAt struct {
	ctx     context.Context
	w       io.WriterAt
	limiter *rate.Limiter
}

// NewRateLimitedWriterAt creates a new RateLimitedWriterAt which respects "limiter" in terms of the number of bytes
// written.
func NewRateLimitedWriterAt(ctx context.Context, w io.WriterAt, limiter *rate.Limiter) *RateLimitedWriterAt {
	return &RateLimitedWriterAt{ctx: ctx, w: w, limiter: limiter}
}

// WriteAt will write from p at off whilst respecting the rate limit.
func (w *RateLimitedWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.w.WriteAt(p, off)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(w.ctx, w.limiter, n)
}

// RateLimitedWriteAtSeeker will use its limiter as a rate limit on the number of bytes written.
type RateLimitedWriteAtSeeker struct {
	ctx     context.Context
	w       ioiface.WriteAtSeeker
	limiter *rate.Limiter
}

// NewRateLimitedWriteAtSeeker creates a new RateLimitedWriteAtSeeker which respects "limiter" in terms of the number
// of bytes written.
func NewRateLimitedWriteAtSeeker(ctx context.Context, w ioiface.WriteAtSeeker, limiter *rate.Limiter,
) *RateLimitedWriteAtSeeker {
	return &RateLimitedWriteAtSeeker{ctx: ctx, w: w, limiter: limiter}
}

// Write will write from p whilst respecting the rate limit.
func (w *RateLimitedWriteAtSeeker) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(w.ctx, w.limiter, n)
}

// WriteAt will write from p at off whilst respecting the rate limit.
func (w *RateLimitedWriteAtSeeker) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.w.WriteAt(p, off)
	if n <= 0 {
		return n, err
	}

	return n, waitChunked(w.ctx, w.limiter, n)
}

// Seek sets the offset for the next write.
func (w *RateLimitedWriteAtSeeker) Seek(offset int64, whence int) (int64, error) {
	return w.w.Seek(offset, whence)
}

// waitChunked waits for n tokens in chunks of the limiter's burst size. This is because rate.Limiter will only allow
// at most its burst number of tokens to be drained at once, so if we want to wait for more than several calls to wait
// are required.
func waitChunked(ctx context.Context, limiter *rate.Limiter, n int) error {
	maxChunkSize := limiter.Burst()

	for n > 0 {
		waitFor := min(n, maxChunkSize)
		if lErr := limiter.WaitN(ctx, waitFor); lErr != nil {
			return fmt.Errorf("could not wait for limiter: %w", lErr)
		}

		n -= waitFor
	}

	return nil
}

var (
	_ io.Reader            = (*RateLimitedReader)(nil)
	_ io.ReaderAt          = (*RateLimitedReaderAt)(nil)
	_ ioiface.ReadAtSeeker = (*RateLimitedReadAtSeeker)(nil)
	_ io.ReadCloser        = (*RateLimitedReadCloser)(nil)
	_ io.ReadSeeker        = (*RateLimitedReadSeeker)(nil)

	_ io.Writer             = (*RateLimitedWriter)(nil)
	_ io.WriterAt           = (*RateLimitedWriterAt)(nil)
	_ ioiface.WriteAtSeeker = (*RateLimitedWriteAtSeeker)(nil)
)
