package maps

// Difference returns all the elements in 'a' that are not in 'b'.
func Difference[M ~map[K]V, K comparable, V any](a, b M) M {
	if len(a) == 0 {
		return make(map[K]V)
	}

	return Filter(a, func(k K, _ V) bool { _, ok := b[k]; return !ok })
}
