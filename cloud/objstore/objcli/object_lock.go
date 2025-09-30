package objcli

import (
	"time"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
)

// ObjectLock represents an object lock.
type ObjectLock struct {
	// Type is the type of the object lock.
	Type objval.LockType
	// Expiration is the time the lock period expires.
	Expiration time.Time
}

// NewComplianceLock creates a new 'compliance' mode object lock.
func NewComplianceLock(expiration time.Time) *ObjectLock {
	return &ObjectLock{Type: objval.LockTypeCompliance, Expiration: expiration}
}
