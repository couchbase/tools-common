package slices

import (
	"github.com/couchbase/tools-common/functional/maps"
)

// Intersection returns all the elements that are in both 'a' and 'b'.
//
// NOTE: The provided slices may contain duplicates but the returned slice will not.
func Intersection[S []E, E comparable](a, b S) S {
	intersection := make(map[E]*intersectionSentinal)

	for _, v := range a {
		intersection[v] = &intersectionSentinal{}
	}

	toggle := func(v E) {
		val, ok := intersection[v]
		if !ok {
			return
		}

		val.both = true
	}

	for _, v := range b {
		toggle(v)
	}

	return maps.Keys(intersection, func(_ E, s *intersectionSentinal) bool { return s.both })
}

// intersectionSentinal is defined to allow the 'Intersection' to be O(n) by signifying existence in both slices without
// linear lookup in the second input slice.
//
// NOTE: Defined globally rather than in 'Intersection' because Go 1.18 doesn't support type declarations inside generic
// functions.
type intersectionSentinal struct {
	both bool
}
