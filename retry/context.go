package retry

import (
	"context"
)

// Context wraps the 'context.Context' interface whilst allowing access to useful attributes such as the number of
// attempts made so far.
type Context struct {
	context.Context
	attempt int
}

// NewContext wraps the given context with a retry context.
func NewContext(ctx context.Context) *Context {
	return &Context{Context: ctx, attempt: 1}
}

// Attempt returns the current attempt number.
func (c *Context) Attempt() int {
	return c.attempt
}
