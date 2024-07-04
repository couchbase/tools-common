package objerr

import (
	"errors"
	"fmt"
)

// ErrArchiveStorage indicates that 'Key' is in an archive tier of storage, e.g. Glacier.
type ErrArchiveStorage struct {
	Key string
}

func (e *ErrArchiveStorage) Error() string {
	return fmt.Sprintf("object '%s' is in long-term storage (e.g. AWS S3 Glacier, Azure Blob Storage Archive) and so "+
		"cannot be accessed", e.Key)
}

// IsErrArchiveStorage returns a boolean indicating whether the given error is a 'ErrArchiveStorage'.
func IsErrArchiveStorage(err error) bool {
	var archiveStorageError *ErrArchiveStorage
	return errors.As(err, &archiveStorageError)
}
