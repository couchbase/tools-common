package objazure

import "errors"

// ErrFailedToDetermineAccountName - Returned in the event that we fail to determine the Azure account name using either
// the static credentials or the environment.
var ErrFailedToDetermineAccountName = errors.New("failed to determine account name")
