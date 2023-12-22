// Package slices provides generic slice utility functions.
package slices

import "golang.org/x/exp/constraints"

// Sum returns the summation of the elements in the provided slice.
func Sum[S ~[]E, E constraints.Ordered](s S) E {
	var total E

	for _, e := range s {
		total += e
	}

	return total
}
