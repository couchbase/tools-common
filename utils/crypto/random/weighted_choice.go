package random

import "github.com/couchbase/tools-common/utils/maths"

// WeightedChoiceOption pairs a type with a weight.
type WeightedChoiceOption[T any] struct {
	// Weight of the option, a higher weight means it's more likely to be selected.
	//
	// NOTE: The sum of the overall weights must be less than 'math.MaxInt'.
	Weight uint

	// Option that may be picked.
	Option T
}

// WeightedChoice returns an element from the given slice of options where elements with a higher weight are more likely
// to be selected.
func WeightedChoice[T any](s []WeightedChoiceOption[T]) (T, error) {
	switch len(s) {
	case 0:
		return *new(T), ErrChoiceIsEmpty
	case 1:
		return s[0].Option, nil
	}

	var total int

	for _, e := range s {
		total += int(e.Weight)
	}

	n, err := Integer(0, total)
	if err != nil {
		return *new(T), err
	}

	var i int

	for ; i < len(s) && n > 0; i++ {
		n -= int(s[i].Weight)
	}

	return s[maths.Max(0, i-1)].Option, nil
}
