package objval

// LockType represents the possible lock types for an object
type LockType string

const (
	LockTypeCompliance LockType = "COMPLIANCE"
	LockTypeUndefined  LockType = ""
)
