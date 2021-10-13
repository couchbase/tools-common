package objval

import (
	"bytes"
	"fmt"
	"strconv"
)

// InvalidByteRangeError is returned if a byte range is invalid for some reason.
type InvalidByteRangeError struct {
	ByteRange *ByteRange
}

// Error implements the 'error' interface.
func (e *InvalidByteRangeError) Error() string {
	return fmt.Sprintf("invalid byte range %d-%d", e.ByteRange.Start, e.ByteRange.End)
}

// ByteRange represents a byte range of an object in the HTTP range header format. For more information on the format
// see 'https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35'.
type ByteRange struct {
	Start int64
	End   int64
}

// Valid returns an 'InvalidByteRangeError' error if the byte range is invalid, <nil> otherwise.
func (b *ByteRange) Valid(required bool) error {
	if (b == nil && !required) || (b != nil && b.End >= b.Start) {
		return nil
	}

	return &InvalidByteRangeError{ByteRange: b}
}

// String implements the 'Stringer' interface, the format will be the HTTP range header format.
func (b *ByteRange) String() string {
	buffer := bytes.NewBufferString(strconv.FormatInt(b.Start, 10) + "-")

	if b.End != 0 {
		buffer.WriteString(strconv.FormatInt(b.End, 10))
	}

	return buffer.String()
}
