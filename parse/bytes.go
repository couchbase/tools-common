package parse

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Bytes parses the given string and returns the number of bytes it represents in a similar fashion to 'ParseDuration'.
//
// NOTE: When no unit suffix is provided, defaults to parsing the number of bytes.
func Bytes(s string) (uint64, error) {
	s = strings.ToLower(s)

	// bs matches and groups different pars of a byte string.
	// For example:
	// 0.5MiB
	// Match 1: 0.5MiB
	//   Group 1: 0.5
	//   Group 2: 0.
	//   Group 3: MiB
	//   Group 4: M
	bs := regexp.MustCompile(`^((\d+\.)?\d+) ?(b|(k|m|g|t|p|e)i?b)?$`)

	g := bs.FindStringSubmatch(s)
	if g == nil || len(g) != 5 {
		return 0, fmt.Errorf("unexpected format")
	}

	q, err := strconv.ParseFloat(g[1], 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse quantifier")
	}

	var m float64 = 1000
	if strings.HasSuffix(g[3], "ib") {
		m = 1024
	}

	var p float64 = 0

	switch g[4] {
	case "k":
		p = 1
	case "m":
		p = 2
	case "g":
		p = 3
	case "t":
		p = 4
	case "p":
		p = 5
	case "e":
		p = 6
	}

	return uint64(q * math.Pow(m, p)), nil
}
