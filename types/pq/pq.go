// Package pq exposes a generic priority queue implemented using a heap.
package pq

import "container/heap"

// PriorityQueue implements a basic priority queue which accepts a generic payload with an integer priority.
type PriorityQueue[T any] struct {
	inner pq[T]
}

// NewPriorityQueue creates a new priority queue where the underlying capacity is set to the given value.
//
// NOTE: The 'PriorityQueue' capacity has the same behavior as a slices capacity meaning it may grow beyond the given
// capacity, the capacity is there for performance optimizations.
func NewPriorityQueue[T any](capacity int) *PriorityQueue[T] {
	return &PriorityQueue[T]{inner: make(pq[T], 0, capacity)}
}

// Enqueue adds the given item to the priority queue.
func (p *PriorityQueue[T]) Enqueue(item Item[T]) {
	heap.Push(&p.inner, item)
}

// Dequeue returns the item from the queue with the highest priority, where multiple items have the same priority,
// they're returned in an arbitrary order.
func (p *PriorityQueue[T]) Dequeue() Item[T] {
	return heap.Pop(&p.inner).(Item[T])
}

// Len returns the number of items in the priority queue.
func (p *PriorityQueue[T]) Len() int {
	return p.inner.Len()
}

// Drain removes all items from the queue running the given function on each item. In the event of an error, dequeuing
// stops early, and returns the error.
func (p *PriorityQueue[T]) Drain(fn func(item Item[T]) error) error {
	for p.Len() > 0 {
		if err := fn(p.Dequeue()); err != nil {
			return err
		}
	}

	return nil
}
