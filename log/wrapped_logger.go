package log

import "fmt"

// WrappedLogger is the internally used Logger struct that implements Logger and defines varous methods for different
// levels of logging, eg: trace, debug, info, etc.
type WrappedLogger struct {
	Logger
}

// NewWrappedLogger returns a WrappedLogger for a given inputted Logger. If logger is nil then assign the nopLogger.
func NewWrappedLogger(logger Logger) WrappedLogger {
	if logger == nil {
		logger = nopLogger{}
	}

	return WrappedLogger{Logger: logger}
}

// Tracef logs the provided information at the trace level.
func (w *WrappedLogger) Tracef(format string, args ...any) {
	w.Log(LevelTrace, format, args...)
}

// Debugf logs the provided information at the debug level.
func (w *WrappedLogger) Debugf(format string, args ...any) {
	w.Log(LevelDebug, format, args...)
}

// Infof logs the provided information at the info level.
func (w *WrappedLogger) Infof(format string, args ...any) {
	w.Log(LevelInfo, format, args...)
}

// Warnf logs the provided information at the warn level.
func (w *WrappedLogger) Warnf(format string, args ...any) {
	w.Log(LevelWarning, format, args...)
}

// Errorf logs the provided information at the error level.
func (w *WrappedLogger) Errorf(format string, args ...any) {
	w.Log(LevelError, format, args...)
}

// Panicf logs the provided information at the panic level.
func (w *WrappedLogger) Panicf(format string, args ...any) {
	w.Log(LevelPanic, format, args...)
	panic(fmt.Sprintf(format, args...))
}
