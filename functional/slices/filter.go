package slices

// Filter removes all elements in the given slice that do not match the given predicates.
//
// NOTE: Providing no predicates results in a no-op.
func Filter[S []E, E any](s S, p ...func(e E) bool) S {
	if len(p) == 0 {
		return s
	}

	filtered := make(S, 0)

	for _, e := range s {
		if filter(e, p...) {
			filtered = append(filtered, e)
		}
	}

	return filtered
}

// filter returns a boolean indicating whether the given element matches all the provided predicates.
func filter[E any](e E, p ...func(e E) bool) bool {
	for _, fn := range p {
		if !fn(e) {
			return false
		}
	}

	return true
}
