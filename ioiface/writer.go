package ioiface

import "io"

// WriteAtSeeker is a composition of the Writer/Seeker/WriterAt at interfaces.
type WriteAtSeeker interface {
	io.Writer
	io.Seeker
	io.WriterAt
}
