// Package batcher groups the items of concurrent calls into batches.
package batcher

import (
	"context"
	"sync"
)

// Func runs a batch of items sharing a key, returning a single error for the whole batch.
type Func[K comparable, T any] func(ctx context.Context, key K, items []T) error

// SplitFunc reports whether to run a failed batch again one item at a time, given the batch's error. Return true when
// the error may have been caused by one of the batch's items, so that item can't fail the others.
//
// NOTE: Only return true for errors which an item can cause, as each rerun is another call.
type SplitFunc func(err error) bool

// Batcher runs a function over the items of concurrent calls sharing a key, so a burst of calls costs one run per
// batch rather than one each. One batch runs per key at a time. Items arriving while it runs join the next batch, up to
// the maximum size.
type Batcher[K comparable, T any] struct {
	// size is the most items in a batch.
	size int

	// fn runs each batch.
	fn Func[K, T]

	// split reports whether a failed batch is run again one item at a time.
	split SplitFunc

	// mu guards queues.
	mu sync.Mutex

	// queues holds the batches waiting for each key; a key is present while its batch runs.
	queues map[K][]*batch[T]
}

// New returns a batcher which runs fn over batches of at most size items.
//
// NOTE: fn and split run on a goroutine of the batcher's own, so a panic in either crashes the process; recover inside
// them if that matters.
func New[K comparable, T any](size int, fn Func[K, T], opts ...Option) *Batcher[K, T] {
	o := newOptions(opts...)

	b := Batcher[K, T]{
		size:   size,
		fn:     fn,
		split:  o.split,
		queues: make(map[K][]*batch[T]),
	}

	return &b
}

// Do adds the item to the key's next batch and waits for that batch to run, returning the item's error.
//
// A batch runs on the context of the call which started it, without its cancellation, as other calls wait on it. A
// call whose context is done stops waiting, but its item still runs.
func (b *Batcher[K, T]) Do(ctx context.Context, key K, item T) error {
	next, idx, running := b.join(key, item)

	// The first call for an idle key runs its batches, later calls wait on them
	if !running {
		go b.run(context.WithoutCancel(ctx), key)
	}

	return next.wait(ctx, idx)
}

// join adds the item to the key's last waiting batch, or a new one when that is full, returning the batch, the item's
// index in it and whether the key's batches were already running.
func (b *Batcher[K, T]) join(key K, item T) (*batch[T], int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue, running := b.queues[key]

	// The last batch is full or there's none waiting, so start another
	if n := len(queue); n == 0 || len(queue[n-1].items) >= b.size {
		queue = append(queue, newBatch[T]())
	}

	next := queue[len(queue)-1]
	idx := next.add(item)

	b.queues[key] = queue

	return next, idx, running
}

// run runs the key's batches in turn, until none are left.
func (b *Batcher[K, T]) run(ctx context.Context, key K) {
	for b.next(ctx, key) {
		// Not a spin: each call blocks running a batch, and returns false once the queue is empty, ending the goroutine
		// until the next call for the key starts another
	}
}

// next runs the key's next batch, returning false once there are none left, in which case the key is forgotten.
func (b *Batcher[K, T]) next(ctx context.Context, key K) bool {
	next, ok := b.pop(key)
	if !ok {
		return false
	}

	next.finish(b.batch(ctx, key, next.items))

	return true
}

// pop removes and returns the key's next batch, forgetting the key when there are none left.
func (b *Batcher[K, T]) pop(key K) (*batch[T], bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue := b.queues[key]
	if len(queue) == 0 {
		delete(b.queues, key)
		return nil, false
	}

	b.queues[key] = queue[1:]

	return queue[0], true
}

// batch runs the items, returning the error for each.
func (b *Batcher[K, T]) batch(ctx context.Context, key K, items []T) []error {
	err := b.fn(ctx, key, items)
	if err == nil {
		return make([]error, len(items))
	}

	if len(items) == 1 || !b.split(err) {
		return repeat(err, len(items))
	}

	return b.calls(ctx, key, items)
}

// calls runs each item as a batch of its own, returning the error for each.
func (b *Batcher[K, T]) calls(ctx context.Context, key K, items []T) []error {
	errs := make([]error, len(items))

	for i, item := range items {
		errs[i] = b.fn(ctx, key, []T{item})
	}

	return errs
}

// repeat returns n copies of err.
func repeat(err error, n int) []error {
	errs := make([]error, n)

	for i := range errs {
		errs[i] = err
	}

	return errs
}
