// Package definitions provides useful error types such as 'MultiError'.
package definitions

import (
	"strings"
)

// MultiError aggregates multiple errors into a single error value.
//
// The zero value of MultiError is ready for use.
//
// NOTE: MultiError is not safe for concurrent use and needs to be wrapped in a lock to be shared safely between
// threads.
type MultiError struct {
	errs []error

	// Prefix will be printed before the errors in this MultiError.
	Prefix string
	// Separator will separate the errors in this MultiError.
	// If omitted, defaults to "; ".
	Separator string
}

// Add adds a new error to this MultiError.
func (m *MultiError) Add(err error) {
	if err == nil {
		return
	}

	m.errs = append(m.errs, err)
}

func (m *MultiError) Error() string {
	if len(m.errs) == 0 {
		return ""
	}

	errStr := strings.Builder{}

	if m.Prefix != "" {
		errStr.WriteString(m.Prefix)
	}

	sep := m.Separator
	if sep == "" {
		sep = "; "
	}

	for _, err := range m.errs[:len(m.errs)-1] {
		errStr.WriteString(err.Error())
		errStr.WriteString(sep)
	}

	errStr.WriteString(m.errs[len(m.errs)-1].Error())

	return errStr.String()
}

// Errors returns the full list of errors accumulated by this MultiError, or nil if there are none.
//
// NOTE: Callers must not modify the returned slice.
func (m *MultiError) Errors() []error {
	return m.errs
}

// ErrOrNil returns this MultiError if it has at least one error, or nil otherwise.
// The intended use case is the following:
//
//	return foo, errs.ErrOrNil()
//
// instead of:
//
//	if len(errs.Errors()) > 0 {
//		return nil, errs
//	}
//
//	return foo, nil
func (m *MultiError) ErrOrNil() error {
	if len(m.errs) > 0 {
		return m
	}

	return nil
}
