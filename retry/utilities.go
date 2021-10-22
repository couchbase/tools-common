package retry

// fibN returns the nth fibonacci number.
func fibN(n int) uint64 {
	if n == 0 {
		return 0
	}

	var (
		first  uint64 = 1
		second uint64 = 1
		temp   uint64
	)

	for i := 2; i < n; i++ {
		temp = second
		second = first + temp
		first = temp
	}

	return second
}
