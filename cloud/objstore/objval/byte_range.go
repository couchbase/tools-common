package objval

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// ErrByteRangeRequired is returned when a function which requires a byte range hasn't been provided with one.
var ErrByteRangeRequired = errors.New("a byte range is required but hasn't been provided")

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

// Valid returns an error if the byte range is invalid, <nil> otherwise.
func (b *ByteRange) Valid(required bool) error {
	if b == nil && required {
		return ErrByteRangeRequired
	}

	if b == nil || b.End == 0 || b.End >= b.Start {
		return nil
	}

	return &InvalidByteRangeError{ByteRange: b}
}

// ToOffsetLength returns the offset/length representation of this byte range.
func (b *ByteRange) ToOffsetLength(length int64) (int64, int64) {
	offset := b.Start

	if b.End != 0 {
		length = b.End - offset + 1
	}

	return offset, length
}

// ToRangeHeader returns the HTTP range header representation of this byte range.
func (b *ByteRange) ToRangeHeader() string {
	buffer := &bytes.Buffer{}

	buffer.WriteString("bytes=")
	buffer.WriteString(strconv.FormatInt(b.Start, 10) + "-")

	if b.End != 0 {
		buffer.WriteString(strconv.FormatInt(b.End, 10))
	}

	return buffer.String()
}
