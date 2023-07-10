package slices

// Subset returns a boolean indicating whether a, is a subset of b.
//
// NOTE: Returns true if a has zero elements since an empty set is a subset of all sets.
func Subset[S []E, E comparable](a, b S) bool {
	if len(a) == 0 {
		return true
	}

	if len(b) == 0 {
		return false
	}

	conv := make(map[E]struct{})

	for _, e := range b {
		conv[e] = struct{}{}
	}

	for _, e := range a {
		if _, ok := conv[e]; !ok {
			return false
		}
	}

	return true
}
