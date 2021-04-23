package fsutil

import "errors"

var (
	// ErrNotFile is returned by 'FileExists' if a directory exists at the provided path.
	ErrNotFile = errors.New("not a file")

	// ErrNotDir is returned by 'DirExists' if a file exists at the provided path.
	ErrNotDir = errors.New("not a directory")
)
