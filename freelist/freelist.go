package freelist

import "context"

// FreeList is a thread-safe pool of objects which one can borrow items from. It has the advantage over sync.Pool that
// the user can control how many objects are in the pool.
//
// NOTE: Internally we use a channel as a buffer. This was found to have the best performance when contention was low,
// and reasonable performance when contention was high.
type FreeList[T any] struct {
	list chan T
}

// NewFreeList creates a new free list.
func NewFreeList[T any](size int) FreeList[T] {
	return FreeList[T]{list: make(chan T, size)}
}

func NewFreeListWithFactory[T any](size int, factory func() T) FreeList[T] {
	fl := NewFreeList[T](size)

	for i := 0; i < size; i++ {
		fl.Put(context.Background(), factory()) //nolint:errcheck
	}

	return fl
}

// Get borrows an object from the freelist. It will block until an object becomes available.
func (f *FreeList[T]) Get(ctx context.Context) (T, error) {
	select {
	case v := <-f.list:
		return v, nil
	case <-ctx.Done():
		return *new(T), ctx.Err()
	}
}

// Put adds v back to the freelist. It can also be used to setup the objects in the pool.
func (f *FreeList[T]) Put(ctx context.Context, v T) error {
	select {
	case f.list <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryGet returns an object if there is one available in the freelist or the default value of T if not. The bool return
// value indicates whether there was an object available. If the bool is false then the value should not be used in any
// way.
func (f *FreeList[T]) TryGet() (T, bool) {
	select {
	case v := <-f.list:
		return v, true
	default:
		return *new(T), false
	}
}

// Count returns the number of available objects currently in the list.
func (f *FreeList[T]) Count() int {
	return len(f.list)
}

// Size returns the maximum number of objects that can be in the list.
func (f *FreeList[T]) Size() int {
	return cap(f.list)
}
