// Package deque provides a double ended queue implemented using a ring buffer.
package deque

import (
	"github.com/couchbase/tools-common/types/v2/ringbuf"
)

const (
	// defaultInitialCapacity defines the initial capacity of the ringbuf.
	defaultInitialCapacity = 2

	// growthFactor is the factor by which the ringbuf capacity increases when we have to grow it.
	//
	// NOTE: This value was chosen because it is a common value that languages use to increase capacity of their dynamic
	// array types. Go itself uses this value until the capacity is 1024 (at which point it grows by a smaller amount).
	growthFactor = 2
)

// IterFunc is a function which will be executed for every element in the deque.
type IterFunc[T any] func(v T)

// Deque is a double-ended queue. It has efficient (i.e. constant time) pop and push to both ends.
//
// NOTE: It is currently implemented as a circular buffer but this detail should not be relied on.
type Deque[T any] struct {
	rb ringbuf.Ringbuf[T]
}

// NewDeque creates a deque of Ts with a default capacity.
func NewDeque[T any]() *Deque[T] {
	return NewDequeWithCapacity[T](defaultInitialCapacity)
}

// NewDequeWithCapacity creates a new deque of Ts with the given initial capacity.
func NewDequeWithCapacity[T any](capacity int) *Deque[T] {
	return &Deque[T]{rb: ringbuf.NewRingbuf[T](capacity)}
}

// Len returns the number of items currently in the deque.
func (d *Deque[T]) Len() int {
	return d.rb.Len()
}

// reallocIfRequired will create a new Ringbuf, with a capacity grown by growthFactor, and copy the existing items
// across if the existing ringbuf is full.
func (d *Deque[T]) reallocIfRequired() {
	if !d.rb.Full() {
		return
	}

	var (
		oldLen = d.rb.Len()
		newRb  = ringbuf.NewRingbuf[T](oldLen * growthFactor)
	)

	d.rb.Iter(func(v T) { newRb.PushBack(v) })

	d.rb = newRb
}

// PushBack adds v to the end of the deque.
func (d *Deque[T]) PushBack(v T) {
	d.reallocIfRequired()
	d.rb.PushBack(v)
}

// PushFront adds v to the start of the deque.
func (d *Deque[T]) PushFront(v T) {
	d.reallocIfRequired()
	d.rb.PushFront(v)
}

// PopBack pops an item from the back of the deque, returning the default value and false if it is empty.
func (d *Deque[T]) PopBack() (T, bool) {
	return d.rb.PopBack()
}

// PopBack pops an item from the front of the deque, returning the default value and false if it is empty.
func (d *Deque[T]) PopFront() (T, bool) {
	return d.rb.PopFront()
}

// Clear removes all items from the deque.
func (d *Deque[T]) Clear() {
	for {
		if _, ok := d.PopFront(); !ok {
			break
		}
	}
}

// Iter calls fn on each item in the deque, starting from the front.
func (d *Deque[T]) Iter(fn IterFunc[T]) {
	d.rb.Iter(ringbuf.IterFunc[T](fn))
}
