package pqutil

// Item encapsulates a payload and its priority.
type Item[T any] struct {
	Payload  T
	Priority int
}
