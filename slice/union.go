package slice

// UnionString returns a slice of elements that are present in both input slices.
//
// NOTE: The returned slice will not contain any duplicates.
func UnionString(a, b []string) []string {
	unionMap := make(map[string]uint8)

	for _, slice := range [][]string{a, b} {
		for _, v := range slice {
			unionMap[v]++
		}
	}

	union := make([]string, 0, len(unionMap))

	for val, count := range unionMap {
		if count > 1 {
			union = append(union, val)
		}
	}

	return union
}
