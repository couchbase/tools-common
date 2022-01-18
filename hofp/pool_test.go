package hofp

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/system"
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

	require.NoError(t, pool.Queue(func() error { executed = true; return nil }))
	require.NoError(t, pool.Stop())
	require.True(t, executed)
}

func TestPoolWorkWithError(t *testing.T) {
	var (
		err      = errors.New("error")
		executed bool
		pool     = NewPool(Options{Size: 1})
	)

	require.NoError(t, pool.Queue(func() error { executed = true; return err }))
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
		require.NoError(t, pool.Queue(func() error { atomic.AddUint64(&executed, 1); return nil }))
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
	require.ErrorIs(t, pool.Queue(func() error { executed = true; return nil }), err)
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
