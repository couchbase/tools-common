package objerr

import (
	"fmt"
)

type ErrDeleteDirectoryRemainingItems struct {
	Bucket string
	Key    string
}

func (e ErrDeleteDirectoryRemainingItems) Error() string {
	return fmt.Sprintf("cannot delete object %q in bucket %q due to its retention policy", e.Key, e.Bucket)
}
