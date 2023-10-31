package random

// Selection returns a slice of 'n' random entries from the given slice.
//
// NOTE: This should not be used for password generation.
func Selection[S ~[]E, E any](s S, n int) (S, error) {
	choices := make(S, 0, n)

	for i := 0; i < n; i++ {
		choice, err := Choice(s)
		if err != nil {
			return nil, err
		}

		choices = append(choices, choice)
	}

	return choices, nil
}
