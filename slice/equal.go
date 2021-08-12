package slice

// EqualStrings - Returns a boolean indicating whether the given strings slices are the same.
//
// NOTE: This function considers:
// 1) that a <nil>/zero length slices are equal
// 2) that unsorted slices are not equal (because we shouldn't modify the given slices)
func EqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
