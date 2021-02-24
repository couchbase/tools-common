package slice

// ContainsString returns a boolean indicating whether the provided slice of strings contains the given string.
func ContainsString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}

// ContainsInt returns a boolean indicating whether the provided slice of ints contains the given int.
func ContainsInt(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}

// ContainsUint16 returns a boolean indicating whether the provided slice of uint16s contains the given uint16.
func ContainsUint16(s []uint16, e uint16) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
