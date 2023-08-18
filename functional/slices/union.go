package slices

import "github.com/couchbase/tools-common/functional/maps"

// Union returns all the elements that are in either 'a' or 'b'.
//
// NOTE: The provided slices may contain duplicates but the returned slice will not.
func Union[S []E, E comparable](a, b S) S {
	union := make(map[E]struct{})

	for _, e := range a {
		union[e] = struct{}{}
	}

	for _, e := range b {
		union[e] = struct{}{}
	}

	return maps.Keys(union)
}
