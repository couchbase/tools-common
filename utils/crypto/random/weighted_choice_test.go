package random

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestWeightedChoice(t *testing.T) {
	elements := []WeightedChoiceOption[int]{
		{
			Weight: 1,
			Option: 1,
		},
		{
			Weight: 2,
			Option: 2,
		},
		{
			Weight: 3,
			Option: 3,
		},
	}

	e, err := WeightedChoice(elements)
	require.NoError(t, err)

	found := slices.ContainsFunc(elements, func(o WeightedChoiceOption[int]) bool {
		return o.Option == e
	})

	require.True(t, found)
}

func TestWeightedChoiceWhenEmpty(t *testing.T) {
	e, err := WeightedChoice([]WeightedChoiceOption[int]{})
	require.ErrorIs(t, err, ErrChoiceIsEmpty)
	require.Zero(t, e)
}
