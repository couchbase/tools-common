package batcher

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// doAll runs the items through a batcher of the given size concurrently, once the first has started its batch; the
// first batch blocks until the rest are queued, so the batches are deterministic. It returns the batches run.
func doAll(t *testing.T, size int, items []int, err error) [][]int {
	var (
		mu      sync.Mutex
		batches [][]int
		release = make(chan struct{})
	)

	b := New(size, func(_ context.Context, key string, items []int) error {
		require.Equal(t, "key", key)

		mu.Lock()
		first := len(batches) == 0
		batches = append(batches, slices.Sorted(slices.Values(items)))
		mu.Unlock()

		if first {
			<-release
		}

		return err
	})

	errs := make(chan error, len(items))

	do := func(item int) {
		errs <- b.Do(context.Background(), "key", item)
	}

	go do(items[0])
	synctest.Wait()

	for _, item := range items[1:] {
		go do(item)
	}

	synctest.Wait()
	close(release)

	for range items {
		require.Equal(t, err, <-errs)
	}

	// Once its last batch has run, the key is forgotten
	synctest.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()

	require.Empty(t, b.queues)

	return batches
}

func TestBatcherBatchesWhileRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The first runs alone, the rest arrive while it runs so share the next batch
		require.Equal(t, [][]int{{0}, {1, 2, 3}}, doAll(t, 10, []int{0, 1, 2, 3}, nil))
	})
}

func TestBatcherSplitsFullBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batches := doAll(t, 2, []int{0, 1, 2, 3, 4}, nil)

		require.Len(t, batches, 3)
		require.Equal(t, []int{0}, batches[0])
		require.Len(t, batches[1], 2)
		require.Len(t, batches[2], 2)
	})
}

func TestBatcherReturnsBatchError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		doAll(t, 10, []int{0, 1, 2}, errors.New("failed"))
	})
}

func TestBatcherKeysRunApart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})

		b := New(10, func(_ context.Context, key string, _ []int) error {
			if key == "blocked" {
				<-release
			}

			return nil
		})

		go func() { _ = b.Do(context.Background(), "blocked", 0) }()
		synctest.Wait()

		// Not held up by the other key's batch
		require.NoError(t, b.Do(context.Background(), "free", 1))

		close(release)
	})
}

func TestBatcherCallerCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			release = make(chan struct{})
			ran     = make(chan []int, 1)
		)

		b := New(10, func(ctx context.Context, _ string, items []int) error {
			<-release

			// Not cancelled with the call which started it
			require.NoError(t, ctx.Err())

			ran <- items

			return nil
		})

		ctx, cancel := context.WithCancel(context.Background())

		errs := make(chan error, 1)

		go func() { errs <- b.Do(ctx, "key", 0) }()
		synctest.Wait()

		cancel()
		require.ErrorIs(t, <-errs, context.Canceled)

		// The item still runs
		close(release)
		require.Equal(t, []int{0}, <-ran)
	})
}

func TestBatcherQueuedCallerCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			release = make(chan struct{})
			ran     = make(chan []int, 2)
		)

		b := New(10, func(_ context.Context, _ string, items []int) error {
			// Hold the first batch, so the cancelled call's item waits in the next
			if slices.Contains(items, 0) {
				<-release
			}

			ran <- items

			return nil
		})

		go func() { _ = b.Do(context.Background(), "key", 0) }()
		synctest.Wait()

		ctx, cancel := context.WithCancel(context.Background())

		errs := make(chan error, 1)

		go func() { errs <- b.Do(ctx, "key", 1) }()
		synctest.Wait()

		// Stops waiting while its item is still queued
		cancel()
		require.ErrorIs(t, <-errs, context.Canceled)

		// The item still runs, in the next batch
		close(release)
		require.Equal(t, []int{0}, <-ran)
		require.Equal(t, []int{1}, <-ran)
	})
}

func TestBatcherNilSplitDoesNotSplit(t *testing.T) {
	var batches int

	b := New(10, func(context.Context, string, []int) error {
		batches++
		return errBad
	}, WithSplit(nil))

	require.Equal(t, []error{errBad, errBad}, runBatch(b, 1, 2))
	require.Equal(t, 1, batches)
}

func TestBatcherDoesNotSplitByDefault(t *testing.T) {
	var batches int

	b := New(10, func(context.Context, string, []int) error {
		batches++
		return errBad
	})

	require.Equal(t, []error{errBad, errBad}, runBatch(b, 1, 2))
	require.Equal(t, 1, batches)
}

// errBad is the error an item can cause, which splits its batch.
var errBad = errors.New("bad item")

// splitBatcher returns a batcher which fails any batch holding a negative item with errBad, and any other batch of more
// than one item with failure, recording the batches run.
func splitBatcher(failure error) (*Batcher[string, int], *[][]int) {
	var batches [][]int

	b := New(10, func(_ context.Context, _ string, items []int) error {
		batches = append(batches, items)

		if slices.ContainsFunc(items, func(i int) bool { return i < 0 }) {
			return errBad
		}

		if len(items) > 1 {
			return failure
		}

		return nil
	}, WithSplit(func(err error) bool { return errors.Is(err, errBad) }))

	return b, &batches
}

// runBatch runs the items as one batch through the batcher, returning each item's error.
func runBatch(b *Batcher[string, int], items ...int) []error {
	return b.batch(context.Background(), "key", items)
}

func TestBatcherSplitsRejectedBatch(t *testing.T) {
	b, batches := splitBatcher(nil)

	// Only the bad item fails, the others run alone
	require.Equal(t, []error{nil, errBad, nil}, runBatch(b, 1, -1, 2))
	require.Equal(t, [][]int{{1, -1, 2}, {1}, {-1}, {2}}, *batches)
}

func TestBatcherDoesNotSplitOtherErrors(t *testing.T) {
	failure := errors.New("throttled")

	b, batches := splitBatcher(failure)

	require.Equal(t, []error{failure, failure}, runBatch(b, 1, 2))
	require.Len(t, *batches, 1)
}

func TestBatcherDoesNotSplitSingleItem(t *testing.T) {
	b, batches := splitBatcher(nil)

	require.Equal(t, []error{errBad}, runBatch(b, -1))
	require.Len(t, *batches, 1)
}

func TestBatcherReturnsItemError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})

		b := New(10, func(_ context.Context, _ string, items []int) error {
			// Hold the first batch, so the rest queue into one
			if slices.Contains(items, 0) {
				<-release
			}

			if slices.ContainsFunc(items, func(i int) bool { return i < 0 }) {
				return errBad
			}

			return nil
		}, WithSplit(func(err error) bool { return errors.Is(err, errBad) }))

		var (
			mu   sync.Mutex
			errs = map[int]error{}
		)

		do := func(item int) {
			err := b.Do(context.Background(), "key", item)

			mu.Lock()
			errs[item] = err
			mu.Unlock()
		}

		go do(0)
		synctest.Wait()

		for _, item := range []int{1, -1, 2} {
			go do(item)
		}

		synctest.Wait()
		close(release)
		synctest.Wait()

		// Each caller gets its own item's error from the split batch
		require.Equal(t, map[int]error{0: nil, 1: nil, -1: errBad, 2: nil}, errs)
	})
}
