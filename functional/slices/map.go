package slices

// Map applies the given function to each element of the provided slice.
func Map[AS ~[]A, BS ~[]B, A, B any](as AS, fn func(a A) B) BS {
	bs := make(BS, 0, len(as))

	for _, a := range as {
		bs = append(bs, fn(a))
	}

	return bs
}
