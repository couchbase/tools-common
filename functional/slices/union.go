package slices

import "github.com/couchbase/tools-common/functional/maps"

// Union returns a slice of elements that are present in both input slices.
//
// NOTE: The returned slice will not contain any duplicates.
func Union[S []E, E comparable](a, b S) S {
	union := make(map[E]uint64)

	for _, slice := range []S{a, b} {
		for _, v := range slice {
			union[v]++
		}
	}

	return maps.Keys(union, func(_ E, v uint64) bool { return v > 1 })
}
