package keygen

import (
	"errors"
	"fmt"
)

// ErrEmptyExpression is returned if the user provides a key generator which is an empty expression.
var ErrEmptyExpression = errors.New("key generator contains an empty expression")

// ExpressionError is returned if the users expression is invalid in some way, the error will contain an index/reason as
// to where/why the error occurred.
type ExpressionError struct {
	index  int
	reason string
}

func (e ExpressionError) Error() string {
	return fmt.Sprintf("error in key expression at char %d, %s", e.index, e.reason)
}

// FieldPathError is returned by 'NewFieldPath' when the provided path is invalid in some way, the error will contain a
// reason as to why the error occurred.
type FieldPathError struct {
	reason string
}

func (e FieldPathError) Error() string {
	return e.reason
}

// ResultError is returned when generating a key for a given document and the given key is invalid, the error will
// contain a reason.
type ResultError struct {
	reason string
}

func (e ResultError) Error() string {
	return fmt.Sprintf("key generation for document failed, %s", e.reason)
}
