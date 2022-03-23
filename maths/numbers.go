package maths

import "golang.org/x/exp/constraints"

// Min returns the smallest of the two elements given as input.
func Min[E constraints.Ordered](a, b E) E {
	if a < b {
		return a
	}

	return b
}

// Max returns the largest of the two elements given as input.
func Max[E constraints.Ordered](a, b E) E {
	if a > b {
		return a
	}

	return b
}
