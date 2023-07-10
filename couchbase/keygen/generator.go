package keygen

import (
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
)

// monoIncrGenerator is a atomic monotonically incrementing number generator which returns integers in base 10.
type monoIncrGenerator struct {
	value uint64
}

var _ keyGenerator = (*monoIncrGenerator)(nil)

func (m *monoIncrGenerator) next(_ []byte) (string, error) {
	return strconv.FormatUint(atomic.AddUint64(&m.value, 1), 10), nil
}

// uuidGenerator is a UUID generator which returns UUID in the V4 format.
type uuidGenerator struct{}

var _ keyGenerator = (*uuidGenerator)(nil)

func (u *uuidGenerator) next(_ []byte) (string, error) {
	return uuid.NewString(), nil
}

// parseGenerator parses a 'keyGenerator' from the provided expression, currently this will be either:
// 1. A monotonically incrementing number generator
// 2. A V4 UUID generator
func parseGenerator(exp string, off int, fDel, gDel rune) (keyGenerator, int, error) {
	var (
		idx      int
		finished bool
	)

	for idx < len(exp) {
		if rune(exp[idx]) == gDel {
			finished = true
			break
		}

		if rune(exp[idx]) == fDel {
			return nil, 0, ExpressionError{off + idx, "attempting to start a field inside a generator"}
		}

		idx++
	}

	if !finished {
		return nil, 0, ExpressionError{off + idx, "unclosed generator at end of expression"}
	}

	generator, err := parseMonoIncr(exp[:idx])
	if err != nil {
		return nil, 0, err
	}

	if generator != nil {
		return generator, idx, nil
	}

	if exp[:idx] == "UUID" {
		return &uuidGenerator{}, idx, nil
	}

	return nil, 0, ExpressionError{off, "invalid generator"}
}

// parseMonoIncrStart attempts to parse a 'monoIncrGenerator' from the provided expression, returns <nil> if the
// provided expression does not contain 'monoIncrGenerator'.
//
// Valid 'MONO_INCR' generators are matches using '^MONO_INCR(\[(\d+)\])?$'. For example when matching 'MONO_INCR[100]':
// Full match: MONO_INCR[100]
// Group 1: [100]
// Group 2: 100
//
// The optional '[\d+]' after 'MONO_INCR' allows the user to create a monotonically incrementing number generator which
// starts at a specific number.
func parseMonoIncr(exp string) (*monoIncrGenerator, error) {
	match := regexp.MustCompile(`^MONO_INCR(\[(\d+)\])?$`).FindStringSubmatch(exp)
	if match == nil {
		return nil, nil
	}

	if match[2] == "" {
		return &monoIncrGenerator{}, nil
	}

	var (
		err   error
		start uint64
	)

	start, err = strconv.ParseUint(match[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MONO_INCR start point '%s'", match[2])
	}

	if start > 0 {
		start--
	}

	return &monoIncrGenerator{value: start}, nil
}
