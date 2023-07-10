package system

// version returns a string representation of the current Windows release.
func version() (string, error) {
	return Execute("ver")
}
