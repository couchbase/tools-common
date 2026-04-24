package kms

// KeyManager is a simple interface so that we can switch between different KMS solutions.
type KeyManager interface {
	// GetRepositoryKey retrieves the key we will use to envelope the different backup keys. This key should never be
	// written to disk unencrypted.
	GetRepositoryKey() ([]byte, error)
}
