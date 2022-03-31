// Package maputil provides basic utility functions for generic maps.
package maputil

import "golang.org/x/exp/maps"

// Keys returns the keys from the given map.
//
// NOTE: When provided with one or more predicates, only returns keys which match all predicates.
func Keys[M ~map[K]V, K comparable, V any](m M, p ...func(k K, v V) bool) []K {
	return maps.Keys(Filter(m, p...))
}

// Values returns the values from the given map.
//
// NOTE: When provided with one or more predicates, only returns values which match all predicates.
func Values[M ~map[K]V, K comparable, V any](m M, p ...func(k K, v V) bool) []V {
	return maps.Values(Filter(m, p...))
}
