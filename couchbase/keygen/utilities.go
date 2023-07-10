package keygen

import (
	"strings"
)

// shouldParse returns a boolean indicating whether we should attempt to parse a generator which uses the provided
// delimiter from the current position in the expression.
func shouldParse(exp string, idx int, del rune) bool {
	n, ok := next(exp, idx)
	return ok && rune(exp[idx]) == del && n != del
}

// next returns the next character if one exists, the return boolean indicates whether a character existed or not.
func next(exp string, idx int) (rune, bool) {
	if len(exp) > idx+1 {
		return rune(exp[idx+1]), true
	}

	return 0, false
}

// unescape reverse escapes all the provided delimiters in the given expression.
func unescape(exp string, del ...rune) string {
	for _, d := range del {
		exp = strings.Replace(exp, string(d)+string(d), string(d), -1)
	}

	return exp
}

// startAtEndMsg is a utility which returns the reason which will be used when a field/generator has been started at the
// end of an expression.
func startAtEndMsg(curr, gDel rune) string {
	if curr == gDel {
		return "start of generator at end of expression"
	}

	return "start of field at end of expression"
}
