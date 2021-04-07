package keygen

// text represents a static text generator, each successive call to 'next' will return the same key. This generator is
// only useful when used in conjunction with other generators.
type text string

var _ keyGenerator = text("")

// parseText parses a static text generator from the provided expression.
func parseText(exp string, off int, fDel, gDel rune) (text, int, error) {
	var idx, end int

	for idx < len(exp) {
		if rune(exp[idx]) != fDel && rune(exp[idx]) != gDel {
			idx++
			end = idx

			continue
		}

		n, ok := next(exp, idx)
		if !ok {
			return "", idx, ExpressionError{off + idx + 1, startAtEndMsg(rune(exp[idx]), gDel)}
		}

		if n != rune(exp[idx]) {
			break
		}

		idx += 2
		end = idx
	}

	return text(unescape(exp[:end], fDel, gDel)), end, nil
}

func (t text) next(_ []byte) (string, error) {
	return string(t), nil
}
