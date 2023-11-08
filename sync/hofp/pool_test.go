package hofp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/utils/v2/system"
)

func TestNewPool(t *testing.T) {
	pool := NewPool(Options{Size: 1})
	require.Equal(t, pool.opts.Size, 1)
	require.Equal(t, "(hofp)", pool.opts.LogPrefix)
	require.NotNil(t, pool.ctx)
	require.NotNil(t, pool.cancel)
	require.Equal(t, 1, cap(pool.hofs))
	require.NoError(t, pool.Stop())
}

func TestPoolWork(t *testing.T) {
	var (
		executed bool
		pool     = NewPool(Options{Size: 1})
	)

	require.NoError(t, pool.Queue(func(_ context.Context) error { executed = true; return nil }))
	require.NoError(t, pool.Stop())
	require.True(t, executed)
}

func TestPoolWorkWithError(t *testing.T) {
	var (
		err      = errors.New("error")
		executed bool
		pool     = NewPool(Options{Size: 1})
	)

	require.NoError(t, pool.Queue(func(_ context.Context) error { executed = true; return err }))
	require.ErrorIs(t, pool.Stop(), err)
	require.True(t, executed)

	// Subsequent calls should return the same error
	require.ErrorIs(t, pool.Stop(), err)
}

func TestPoolSize(t *testing.T) {
	pool := NewPool(Options{Size: 1})
	require.Equal(t, 1, pool.Size())
}

func TestPoolQueue(t *testing.T) {
	var (
		executed uint64
		pool     = NewPool(Options{Size: system.NumCPU()})
	)

	require.Equal(t, system.NumCPU(), pool.Size())

	for i := 0; i < 42; i++ {
		require.NoError(t, pool.Queue(func(_ context.Context) error { atomic.AddUint64(&executed, 1); return nil }))
	}

	require.NoError(t, pool.Stop())
	require.Equal(t, uint64(42), executed)
}

func TestPoolQueueAfterTearDown(t *testing.T) {
	var (
		executed bool
		err      = errors.New("error")
		pool     = NewPool(Options{Size: 1})
	)

	require.True(t, pool.setErr(err))
	require.ErrorIs(t, pool.Queue(func(_ context.Context) error { executed = true; return nil }), err)
	require.ErrorIs(t, pool.Stop(), err)
	require.False(t, executed)
}

func TestPoolStop(t *testing.T) {
	pool := NewPool(Options{Size: 1})
	require.NoError(t, pool.Stop())

	// Should not see a double closure of the work channel
	require.NoError(t, pool.Stop())
}

func TestStopAfterTearDown(t *testing.T) {
	var (
		err  = errors.New("error")
		pool = NewPool(Options{Size: 1})
	)

	require.True(t, pool.setErr(err))
	require.ErrorIs(t, pool.Stop(), err)
}

func TestPoolGetErr(t *testing.T) {
	var (
		err  = errors.New("error")
		pool = &Pool{err: err}
	)

	require.ErrorIs(t, pool.getErr(), err)
}

func TestPoolSetErr(t *testing.T) {
	var (
		first  = errors.New("first")
		second = errors.New("second")
		pool   = NewPool(Options{Size: 1})
	)

	require.True(t, pool.setErr(first))
	require.False(t, pool.setErr(second))
	require.ErrorIs(t, pool.Stop(), first)
}

func TestWorkerTeardown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		pool  = NewPool(Options{Context: ctx, Size: 5})
		count uint64
	)

	fn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			atomic.AddUint64(&count, 1)
			return ctx.Err()
		case <-time.After(time.Second):
			return assert.AnError
		}
	}

	for i := 0; i < 5; i++ {
		require.NoError(t, pool.Queue(fn))
	}

	// Give the scheduler time to start processing all the queued functions
	time.Sleep(250 * time.Millisecond)
	cancel()

	require.ErrorIs(t, pool.Stop(), ctx.Err())
	require.Equal(t, uint64(5), count)
}

// Checks that we don't deadlock when queuing functions that immediately fail
// https://issues.couchbase.com/browse/MB-53064
func TestNoDeadlockWhenStillQueuingAfterWorkFails(t *testing.T) {
	var (
		pool = NewPool(Options{Size: 2})
		fn   = func(_ context.Context) error { time.Sleep(time.Millisecond); return assert.AnError }
	)

	for i := 0; i < 100; i++ {
		_ = pool.Queue(fn)
	}

	require.ErrorIs(t, pool.Stop(), assert.AnError)
}
