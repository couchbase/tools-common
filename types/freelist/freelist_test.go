package freelist

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/sync/hofp"
	"github.com/couchbase/tools-common/types/ptr"
)

func TestFreeListLengthOne(t *testing.T) {
	fl := NewFreeList[*int](1)

	_, ok := fl.TryGet()
	require.False(t, ok)

	require.NoError(t, fl.Put(context.Background(), ptr.To(0)))

	v, ok := fl.TryGet()
	require.True(t, ok)
	require.Equal(t, 0, *v)

	*v = 10
	require.NoError(t, fl.Put(context.Background(), v))

	v, ok = fl.TryGet()
	require.True(t, ok)
	require.Equal(t, 10, *v)

	*v = 20
	require.NoError(t, fl.Put(context.Background(), v))
	v, err := fl.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, 20, *v)
}

func TestFreeListMultiple(t *testing.T) {
	const N = 10

	fl := NewFreeList[*int](N)
	require.Equal(t, N, fl.Size())

	for i := 0; i < N; i++ {
		require.NoError(t, fl.Put(context.Background(), ptr.To(i)))
	}

	require.Equal(t, N, fl.Count())

	for i := 0; i < N; i++ {
		v, ok := fl.TryGet()
		require.True(t, ok)
		require.Equal(t, i, *v)
	}

	require.Equal(t, 0, fl.Count())

	_, ok := fl.TryGet()
	require.False(t, ok)
}

func TestFreeListCancellation(t *testing.T) {
	fl := NewFreeList[*int](1)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := fl.Get(ctx)
	require.ErrorIs(t, context.Canceled, err)

	ctx, cancel = context.WithCancel(context.Background())
	require.NoError(t, fl.Put(ctx, ptr.To(0)))

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err = fl.Put(ctx, ptr.To(1))
	require.ErrorIs(t, context.Canceled, err)
}

func TestFreeListWithFactory(t *testing.T) {
	fl := NewFreeListWithFactory(4, func() *int { return new(int) })
	require.Equal(t, fl.Count(), 4)
}

func BenchmarkFreeList(b *testing.B) {
	benches := []struct{ workers, objects int }{
		{workers: 1, objects: 1},
		{workers: 2, objects: 1},
		{workers: 4, objects: 1},
		{workers: 4, objects: 2},
		{workers: 8, objects: 2},
		{workers: 16, objects: 2},
		{workers: 4, objects: 4},
		{workers: 8, objects: 4},
		{workers: 16, objects: 4},
	}

	for _, bench := range benches {
		b.Run(fmt.Sprintf("%d-%d", bench.workers, bench.objects), func(b *testing.B) {
			fl := NewFreeList[*int](bench.objects)

			for i := 0; i < bench.objects; i++ {
				fl.Put(context.Background(), ptr.To(0)) //nolint:errcheck
			}

			for i := 0; i < b.N; i++ {
				runBenchmark(fl, bench.workers)
			}
		})
	}
}

func runBenchmark(fl FreeList[*int], workers int) {
	pool := hofp.NewPool(hofp.Options{Size: workers})

	fn := func(ctx context.Context) error {
		for i := 0; i < 1000; i++ {
			v, _ := fl.Get(context.Background())
			*v = i
			fl.Put(context.Background(), v) //nolint:errcheck
		}

		return nil
	}

	for i := 0; i < pool.Size(); i++ {
		_ = pool.Queue(fn)
	}

	_ = pool.Stop()
}
