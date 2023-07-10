package evtlog

import "errors"

// ErrTooLarge returned if the encoded event payload is larger than 3KiB.
var ErrTooLarge = errors.New("payload too large")
