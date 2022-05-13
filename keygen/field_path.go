package keygen

import (
	"reflect"
)

// These constants are added for readability as the escaping can make it confusing
const (
	Backtick = '`'
	Period   = '.'
)

// FieldPath represents a path to a (possibly) nested field in a JSON document for example, "nested.field" or when the
// field contains a '.' "another.`not.nested`.field".
type FieldPath []string

// NewFieldPath parses a field reference (e.g parent.child) and returns a FieldPath struct which represents the various
// nested objects in the document.
//
// The syntax rules are as follows:
// 1) Nested fields are separated using a '.' character
// 2) '`' characters may be used to represent an exact string
// 3) You may escape '`' characters by "doubling up" '``' represents a single '`'
// 4) Keys that contain a '.' character but are not nested should be enclosed in '`'
//
// Examples:
// 1) 'key' -> ['key']
// 2) 'nested.key' -> ['nested', 'key']
// 3) '`not.a.nested`.key' -> ['not.a.nested', 'key']
// 4) '```.key`' -> ['`.key']
// 5) '```.key```' -> ['`.key`']
func NewFieldPath(path string) (FieldPath, error) {
	if path[0] == Period {
		return nil, &FieldPathError{"cannot find nested object of field without name"}
	}

	var (
		idx    int
		fields = make([]string, 0)
	)

	for idx < len(path) {
		field, off, err := parseFPField(path, idx)
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
		idx += off
	}

	return fields, nil
}

// RemoveFrom removes the field path from the provided object if it exists. No changes will be made to the document if
// the field does not exist.
func (fp FieldPath) RemoveFrom(object map[string]any) {
	current := object

	for idx := 0; idx < len(fp)-1; idx++ {
		value := current[fp[idx]]

		if value == nil || reflect.TypeOf(value).Kind() != reflect.Map {
			return
		}

		current = value.(map[string]any)
	}

	delete(current, fp[len(fp)-1])
}

// parseFPField parses a single field path field from the provided path. Returns the parsed field and the number of
// bytes consumed from the provided path.
func parseFPField(path string, idx int) (string, int, error) {
	var (
		start = idx
		open  bool
		field string
		off   int
		err   error
	)

	for {
		if idx >= len(path) {
			break
		}

		var char string

		if open {
			char, off, open = parseOpen(path, idx)
		} else {
			char, off, open, err = parseNotOpen(path, idx)
		}

		if err != nil {
			return "", 0, err
		}

		if off == 0 {
			break
		}

		field += char
		idx += off
	}

	if open {
		return "", 0, &FieldPathError{reason: "unbalanced backticks"}
	}

	return field, idx - start + 1, nil
}

// parseOpen parses a single character from the provided path in the open state, returns the character and the number of
// bytes consumed from the input.
func parseOpen(path string, idx int) (string, int, bool) {
	switch path[idx] {
	case Backtick:
		n, ok := next(path, idx)

		// This is an escaped backtick, skip it and continue in the open state
		if ok && n == Backtick {
			return "`", 2, true
		}

		// This is a closing backtick, consume and return to the !open state
		return "", 1, false
	default:
		// Consume the given character and continue in the open state
		return string(path[idx]), 1, true
	}
}

// parseNotOpen parses a single character from the provided path in the not-open state, returns the character and the
// number of bytes consumed from the input.
func parseNotOpen(path string, idx int) (string, int, bool, error) {
	switch path[idx] {
	case Period:
		// We're either at the start of the input or have duplicate '.' characters, we can't address an empty field
		if idx == 0 || path[idx-1] == Period {
			return "", 0, false, &FieldPathError{reason: "empty field name"}
		}

		// This is a special case in that we don't increment the input, this signals the caller that we've finished
		// parsing this field.
		return "", 0, false, nil
	case Backtick:
		n, ok := next(path, idx)

		// This is an escaped backtick, skip it and continue in the !open state
		if ok && n == Backtick {
			return "`", 2, false, nil
		}

		// This is a opening backtick, consume and transition to the open state
		return "", 1, true, nil
	default:
		// Consume the given character and continue in the !open state
		return string(path[idx]), 1, false, nil
	}
}
