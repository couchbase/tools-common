package random

// Choice returns a random element from the given slice.
func Choice[S ~[]E, E any](s S) (E, error) {
	switch len(s) {
	case 0:
		return *new(E), ErrChoiceIsEmpty
	case 1:
		return s[0], nil
	}

	idx, err := Integer(0, len(s)-1)
	if err != nil {
		return *new(E), err
	}

	return s[idx], nil
}
