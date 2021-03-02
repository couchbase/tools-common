package slice

// RemoveStringAt removes the string at index i and returns the modified slice.
//
// NOTE: This function alters the order of the elements.
func RemoveStringAt(s []string, i int) ([]string, error) {
	if i >= len(s) || i < 0 {
		return nil, IndexOutOfRangeError{i: i, length: len(s)}
	}

	// Remove the element at index i by copying the last element to index i and then trimming the last element
	s[i] = s[len(s)-1]

	return s[:len(s)-1], nil
}
