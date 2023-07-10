// Package iface provides useful - generally composition - interface definitions such as 'WriteAtSeeker'.
package iface

import "io"

// WriteAtSeeker is a composition of the Writer/Seeker/WriterAt at interfaces.
type WriteAtSeeker interface {
	io.Writer
	io.Seeker
	io.WriterAt
}
