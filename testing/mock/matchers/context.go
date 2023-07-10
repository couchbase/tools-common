// Package matchers provides useful 'Matcher' implementations for 'testify/mock'.
package matchers

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// Context matches any implementation of 'context.Context'.
var Context = mock.MatchedBy(func(_ context.Context) bool { return true })
