package maths

// MinUint64 returns the smallest of the two uint64s given as input.
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

// MaxUint64 returns the largest of the two uint64s given as input.
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}

// Min returns the smallest of the two ints given as input.
func Min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

// Max returns the largest of the two ints given as input.
func Max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
