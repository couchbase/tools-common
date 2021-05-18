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

// MinInt64 returns the smallest of the two int64s given as input.
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

// MaxInt64 returns the largest of the two int64s given as input.
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

// MinUint32 returns the smallest of the two uint32s given as input.
func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}

	return b
}

// MaxUint32 returns the largest of the two uint32s given as input.
func MaxUint32(a, b uint32) uint32 {
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
