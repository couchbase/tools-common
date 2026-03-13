package maps

// Map applies the given function to each element of the provided map.
func Map[AS ~map[AK]AV, BS ~map[BK]BV, AK, BK comparable, AV, BV any](as AS, fn func(ak AK, av AV) (BK, BV)) BS {
	bs := make(BS)

	for ak, av := range as {
		// Map the key/value
		bk, bv := fn(ak, av)

		// Assign into the new map
		bs[bk] = bv
	}

	return bs
}
