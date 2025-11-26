package objval

// BucketLockingStatus represents the bucket-level object locking metadata.
type BucketLockingStatus struct {
	// Enabled - if set to true then object locking must be enabled for the bucket.
	Enabled bool
}

// BucketVersioningStatus represents the bucket-level object versioning metadata.
type BucketVersioningStatus struct {
	// Enabled - if set to true then object versioning must be enabled for the bucket.
	Enabled bool
}
