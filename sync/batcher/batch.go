package batcher

import "context"

// batch is the items run together.
type batch[T any] struct {
	// items are the items of the calls which joined the batch, in the order they joined.
	items []T

	// done is closed once the batch has run, and errs is set.
	done chan struct{}

	// errs is the error for each item, by index.
	errs []error
}

// newBatch returns an empty batch, waiting to run.
func newBatch[T any]() *batch[T] {
	return &batch[T]{done: make(chan struct{})}
}

// add adds the item to the batch, returning its index.
func (b *batch[T]) add(item T) int {
	b.items = append(b.items, item)
	return len(b.items) - 1
}

// finish sets the error for each item and wakes the batch's callers.
func (b *batch[T]) finish(errs []error) {
	b.errs = errs
	close(b.done)
}

// wait waits for the batch to run, returning the error for the item at idx, or the context's error if it's done first.
func (b *batch[T]) wait(ctx context.Context, idx int) error {
	select {
	case <-b.done:
		return b.errs[idx]
	case <-ctx.Done():
		return ctx.Err()
	}
}
