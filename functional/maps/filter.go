package maps

import "golang.org/x/exp/maps"

// Filter removes all key/value pairs in the given map that do not match the given predicates.
//
// NOTE: Providing no predicates results in a no-op.
func Filter[M ~map[K]V, K comparable, V any](m M, p ...func(k K, v V) bool) M {
	if len(p) == 0 {
		return m
	}

	for _, fn := range p {
		maps.DeleteFunc(m, func(k K, v V) bool { return !fn(k, v) })
	}

	return m
}
