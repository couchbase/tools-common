package log

// nopLogger is the no opporations logger - ie, a nil logger that doesn't log anything.
type nopLogger struct{}

// Log method for the nopLogger which does nothing.
func (n nopLogger) Log(_ Level, _ string, _ ...any) {}
