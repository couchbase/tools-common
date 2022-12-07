package ringbuf

// mod returns numerator % denominator but defined in the Python way.
//
// In Go modulo is defined for negative numbers such that the result is negative. As an example -5 % 3 is -2. For our
// purposes (manipulating indexes) the definition where the result is always non-negative is more useful (i.e. -5 % 3 is
// 1).
func mod(numerator, denominator int) int {
	m := numerator % denominator
	if m < 0 {
		m += denominator
	}

	return m
}

// IterFunc is a function which will be executed for every element in the Ringbuf
type IterFunc[T any] func(v T)

// Ringbuf is a circular buffer of items with type T. It can hold a constant number of items (specifically:
// len(items)-1).
type Ringbuf[T any] struct {
	// head points to the first element in the ringbuf.
	head int

	// tail points to the next free slot at the end of the ringbuf.
	tail int

	items []T
}

// NewRingbuf creates a Ringbuf of T with the specified capacity.
func NewRingbuf[T any](capacity int) Ringbuf[T] {
	// This implementation uses head == tail to mean that the ringbuf is empty. That means when there are items in the
	// ringbuf the head and tail indexes must be at least one apart, and as tail points to the next free slot this means
	// there is always one empty slot, so we actually need capacity+1 slots.
	return Ringbuf[T]{items: make([]T, capacity+1)}
}

// Full returns whether or not it is possible to push another item.
func (r *Ringbuf[T]) Full() bool {
	return r.Len() >= r.Cap()
}

// Empty returns whether or not there are no items in the ringbuf.
func (r *Ringbuf[T]) Empty() bool {
	return r.head-r.tail == 0
}

// Cap returns the capacity of the ringbuf.
func (r *Ringbuf[T]) Cap() int {
	return len(r.items) - 1
}

// Len returns the number of items in the ringbuf currently.
func (r *Ringbuf[T]) Len() int {
	// Ringbufs are circular, which means the start of the list might be in an index after the end of the list. In that
	// case we need to count the elements after the head (len(r.items) - r.head) and then add the items at the beginning
	// of the list up to tail (+ r.tail).
	if r.head > r.tail {
		return len(r.items) - r.head + r.tail
	}

	return r.tail - r.head
}

// PushFront adds v to the front of the ringbuf.
func (r *Ringbuf[T]) PushFront(v T) bool {
	if r.Full() {
		return false
	}

	index := mod(r.head-1, len(r.items))

	r.items[index] = v
	r.head = index

	return true
}

// PushBack adds v to the back of the ringbuf.
func (r *Ringbuf[T]) PushBack(v T) bool {
	if r.Full() {
		return false
	}

	index := mod(r.tail+1, len(r.items))

	r.items[r.tail] = v
	r.tail = index

	return true
}

// PopFront returns a copy of the value at the front of the ringbuf and removes it. If the ringbuf is empty then it
// returns the default value and false for the bool return value.
func (r *Ringbuf[T]) PopFront() (T, bool) {
	if r.Empty() {
		return *new(T), false
	}

	v := r.items[r.head]

	r.head = mod(r.head+1, len(r.items))

	return v, true
}

// PopBack returns a copy of the value at the back of the ringbuf and removes it. If the ringbuf is empty then it
// returns the default value and false for the bool return value.
func (r *Ringbuf[T]) PopBack() (T, bool) {
	if r.Empty() {
		return *new(T), false
	}

	var (
		index = mod(r.tail-1, len(r.items))
		v     = r.items[index]
	)

	r.tail = index

	return v, true
}

// Iter iterates through all the elements in the Ringbuf, calling fn on each one.
func (r *Ringbuf[T]) Iter(fn IterFunc[T]) {
	var (
		start = r.head
		end   = r.tail
	)

	// If the head is after the tail then we need to iterate to the end of the list and then iterate from the beginning
	// up to tail (second for loop in this method).
	if r.head > r.tail {
		end = len(r.items)
	}

	for i := start; i < end; i++ {
		fn(r.items[i])
	}

	if r.tail > r.head {
		return
	}

	for i := 0; i < r.tail; i++ {
		fn(r.items[i])
	}
}
