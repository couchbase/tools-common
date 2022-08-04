package keygen

import (
	"bytes"
	"fmt"
)

// maxKeySize is the maxium size of a generated key; all generated keys longer than 250 bytes will return an error.
const maxKeySize = 250

// keyGenerator is an internal interface which abstracts away the generation allowing the key generator to interact with
// one or more different generators.
type keyGenerator interface {
	next(data []byte) (string, error)
}

// KeyGenerator is a list of generators which can be used in succession to generate a document key.
type KeyGenerator []keyGenerator

// NewKeyGenerator parses a key generator using the provided expression, field and generator delimiters.
//
// The following examples assume the default field/generator delimiters used by 'cbimport' using the document below:
//
//	{
//	  "key": "value1",
//	  "nested": {"key": "value2"},
//	  "nested": {"with.dot": "value3"},
//	}
//
// Supported generators:
//  1. MONO_INCR
//     a. '#MONO_INCR#' -> '0', '1' ...
//     b. '#MONO_INCR[100]#' -> '100', '101' ...
//  2. UUID
//     a. '#UUID#' -> '67a7f0a4-99a5-4607-b275-c3b436250ad2', '130fff70-4c66-4788-9194-5271f18cb0e1' ...
//  3. Static Text
//     a. 'example' -> 'example'
//  4. Field
//     a. '%key%' -> 'value1'
//     b. '%nested.key%' -> 'value2'
//     c. '%nested.`with.dot`%' -> 'value3'
//
// These generators are useful on their own, however, the real power comes from being able to combine them into more
// complex expressions, for example:
// 1. 'key::#MONO_INCR#' -> 'key::1', 'key::2' ...
// 3. 'user-#UUID#' -> 'user-e0837e46-0d48-45e3-92e7-28031170d23d', 'user-f9a8f39a-b63a-44fc-8455-c493a6cedf60' ...
// 3. 'key::#UUID#::#MONO_INCR[50]#%key%' -> key::d9c64d00-9c9a-4191-acf3-d882793eccf5::1::value1 ...
func NewKeyGenerator(exp string, fDel, gDel rune) (KeyGenerator, error) {
	if err := validateDelimiters(fDel, gDel); err != nil {
		return nil, err
	}

	if exp == "" {
		return nil, ErrEmptyExpression
	}

	var (
		idx        int
		generators = make([]keyGenerator, 0)
	)

	for idx < len(exp) {
		generator, off, err := parseKeyGenerator(exp, idx, fDel, gDel)
		if err != nil {
			return nil, err
		}

		generators = append(generators, generator)
		idx += off
	}

	return generators, nil
}

// Next generates a key for the provided document body.
func (k KeyGenerator) Next(data []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))

	for i := 0; i < len(k); i++ {
		part, err := k[i].next(data)
		if err != nil {
			return nil, err
		}

		buffer.WriteString(part)
	}

	if buffer.Len() == 0 {
		return nil, &ResultError{reason: "generated key is an empty string"}
	}

	if buffer.Len() > maxKeySize {
		return nil, &ResultError{reason: fmt.Sprintf("generated key is larger than %d bytes", maxKeySize)}
	}

	return buffer.Bytes(), nil
}

// validateDelimiters will return an error if the provided delimiters are invalid in some way.
func validateDelimiters(fDel, gDel rune) error {
	if fDel == 0 || string(fDel) == "" {
		return fmt.Errorf("field delimiter can not be the empty string")
	}

	if gDel == 0 || string(gDel) == "" {
		return fmt.Errorf("generator delimiter can not be the empty string")
	}

	if gDel == '.' || fDel == '.' {
		return fmt.Errorf("cannot use . as a field or generator delimiter")
	}

	if gDel == '`' || fDel == '`' {
		return fmt.Errorf("cannot use ` as a field or generator delimiter")
	}

	if fDel == gDel {
		return fmt.Errorf("field delimiter and generator delimiter can not be the same")
	}

	return nil
}

// parseKeyGenerator will parse a key generator at the given index in the provided expression. Returns an error if the
// parsing fails (likely due to an invalid expression).
func parseKeyGenerator(exp string, idx int, fDel, gDel rune) (keyGenerator, int, error) {
	if shouldParse(exp, idx, fDel) {
		generator, off, err := parseField(exp[idx+1:], idx+1, fDel)
		return generator, off + 2, err
	}

	if shouldParse(exp, idx, gDel) {
		generator, off, err := parseGenerator(exp[idx+1:], idx+1, fDel, gDel)
		return generator, off + 2, err
	}

	return parseText(exp[idx:], idx, fDel, gDel)
}
