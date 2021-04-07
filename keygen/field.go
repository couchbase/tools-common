package keygen

import (
	jsoniter "github.com/json-iterator/go"
)

// field is a wrapper around a 'FieldPath' which implements the 'keyGenerator' interface allowing values to be extracted
// from a document and used as part of the key.
type field FieldPath

var _ keyGenerator = field{}

// parseField returns a parsed field path from the provided expression.
func parseField(exp string, off int, fDel rune) (field, int, error) {
	var idx int

	for idx < len(exp) {
		if rune(exp[idx]) != fDel {
			idx++

			continue
		}

		// Escaped field delimiter, jump to next character
		if len(exp) > idx+1 && rune(exp[idx+1]) == fDel {
			idx += 2

			continue
		}

		fieldPath, err := NewFieldPath(unescape(exp[:idx], fDel))

		return field(fieldPath), idx, err
	}

	return nil, 0, ExpressionError{off + idx, "unclosed field at end of expression"}
}

// next will return the corresponding value to the field reference as a string if the field exists in the document
// passed as a parameter. If it does not exist it will return an error.
func (f field) next(data []byte) (string, error) {
	converted := make([]interface{}, 0, len(f))
	for _, field := range f {
		converted = append(converted, field)
	}

	any := jsoniter.Get(data, converted...)
	switch any.ValueType() {
	case jsoniter.InvalidValue:
		return "", &ResultError{
			reason: "resulting field does not exist",
		}
	case jsoniter.NilValue:
		return "", &ResultError{
			reason: "resulting field is null",
		}
	case jsoniter.ArrayValue, jsoniter.ObjectValue:
		return "", &ResultError{
			reason: "resulting field is a JSON array/object",
		}
	}

	return any.ToString(), nil
}
