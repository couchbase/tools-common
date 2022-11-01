package log

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
