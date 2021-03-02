package slice

// FindString returns the index of the first occurrence of the element in the slice. If the element is not present it
// returns -1.
func FindString(s []string, e string) int {
	for i, val := range s {
		if val == e {
			return i
		}
	}

	return -1
}
