package slices

// Difference returns all the elements in 'a' that are not in 'b'.
//
// NOTE: The provided slices may contain duplicates but the returned slice will not.
func Difference[S []E, E comparable](a, b S) S {
	lu := make(map[E]struct{})

	for _, e := range b {
		lu[e] = struct{}{}
	}

	return Filter(a, func(e E) bool { _, ok := lu[e]; return !ok })
}
