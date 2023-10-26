package random

import "errors"

// ErrChoiceIsEmpty is returned if the user attempts to choose from an empty slice.
var ErrChoiceIsEmpty = errors.New("can't choose from an empty slice")
