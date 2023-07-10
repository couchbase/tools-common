package objazure

import "errors"

// ErrFailedToDetermineAccountName is returned in the event that we fail to determine the Azure account name using both
// the static credentials or the environment.
var ErrFailedToDetermineAccountName = errors.New("failed to determine account name")
