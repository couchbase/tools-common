package log

import (
	"fmt"
	"time"
)

// StdoutLogger is the standard output logger for printing all logs into the commandline.
type StdoutLogger struct{}

// Log method for the StdoutLogger which adds prefix dependant on the level and prints message inputted to terminal.
func (s StdoutLogger) Log(level Level, msg string, args ...any) {
	var prefix string

	switch level {
	case LevelTrace:
		prefix = "TRAC"
	case LevelDebug:
		prefix = "DEBU"
	case LevelInfo:
		prefix = "INFO"
	case LevelWarning:
		prefix = "WARN"
	case LevelError:
		prefix = "ERRO"
	case LevelPanic:
		prefix = "PNIC"
	}

	fmt.Println(time.RFC3339Nano + " " + prefix + ": " + fmt.Sprintf(msg, args...))
}
