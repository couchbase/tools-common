package log

import "fmt"

// Level is a type alias which is used to indicate the verbosity of an log statement.
type Level uint8

const (
	// LevelTrace is the most verbose log level including finer grained informational events than debug level.
	LevelTrace Level = iota

	// LevelDebug includes fine-grained informational events that are the most useful to debug the library.
	LevelDebug

	// LevelInfo includes informational messages that highlight the progress of events in the library at a
	// course-grained level.
	LevelInfo

	// LevelWarning includes expected but potentially harmful/interesting events.
	LevelWarning

	// LevelError includes error events which may still allow the library to continue running.
	LevelError

	// LevelPanic includes errors events which should lead to a panic. This level will only be used in the most severe
	// of cases.
	LevelPanic
)

// Logger interface which allows applications to provide custom logger implementations.
type Logger interface {
	Log(level Level, format string, args ...any)
}

// logger is the logger which is used internally by the library. Any calls the functions below use/affect this logger.
var logger Logger

// SetLogger sets the logger which will be used by the tools-common library.
func SetLogger(l Logger) {
	logger = l
}

// Logf allows raw access to the underlying logger, most use cases should be through the functions below.
//
// NOTE: If no logger has been set using 'SetLogger' all logging information is omitted.
func Logf(level Level, format string, args ...any) {
	if logger == nil {
		return
	}

	logger.Log(level, format, args...)
}

// Tracef logs the provided information at the trace level.
func Tracef(format string, args ...any) {
	Logf(LevelTrace, format, args...)
}

// Debugf logs the provided information at the debug level.
func Debugf(format string, args ...any) {
	Logf(LevelDebug, format, args...)
}

// Infof logs the provided information at the info level.
func Infof(format string, args ...any) {
	Logf(LevelInfo, format, args...)
}

// Warnf logs the provided information at the warn level.
func Warnf(format string, args ...any) {
	Logf(LevelWarning, format, args...)
}

// Errorf logs the provided information at the error level.
func Errorf(format string, args ...any) {
	Logf(LevelError, format, args...)
}

// Panicf logs the provided information at the panic level.
func Panicf(format string, args ...any) {
	Logf(LevelPanic, format, args...)
	panic(fmt.Sprintf(format, args...))
}
