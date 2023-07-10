// Package log provides an interface to setup logging when using 'tools-common'.
package log

// Logger interface which allows applications to provide custom logger implementations.
type Logger interface {
	Log(level Level, format string, args ...any)
}
