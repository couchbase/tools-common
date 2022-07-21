package testutil

import (
	"context"

	"github.com/stretchr/testify/mock"
)

var MockMatchContext = mock.MatchedBy(func(_ context.Context) bool { return true })
