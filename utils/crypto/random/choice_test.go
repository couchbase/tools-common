package random

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChoice(t *testing.T) {
	elements := []int{1, 2, 3}

	e, err := Choice(elements)
	require.NoError(t, err)
	require.Contains(t, elements, e)
}

func TestChoiceWhenEmpty(t *testing.T) {
	e, err := Choice([]int{})
	require.ErrorIs(t, err, ErrChoiceIsEmpty)
	require.Zero(t, e)
}
