package slice

import "fmt"

// IndexOutOfRangeError is returned when performing a request which the provided index would cause an index out of range
// panic if executed.
type IndexOutOfRangeError struct {
	length int
	i      int
}

func (e IndexOutOfRangeError) Error() string {
	return fmt.Sprintf("index out of range %d with length %d", e.i, e.length)
}
