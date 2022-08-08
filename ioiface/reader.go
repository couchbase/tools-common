package ioiface

import "io"

// ReadAtSeeker is a composition of the reader/seeker/reader at interfaces.
type ReadAtSeeker interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}
