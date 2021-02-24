package slice

// SumFloat64 returns the summation of the float64s in the provided slice.
func SumFloat64(s []float64) float64 {
	var total float64
	for _, e := range s {
		total += e
	}

	return total
}
