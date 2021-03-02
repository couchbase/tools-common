package slice

import "fmt"

type IndexOutOfRangeError struct {
	length int
	i      int
}

func (e IndexOutOfRangeError) Error() string {
	return fmt.Sprintf("index out of range %d with length %d", e.i, e.length)
}
