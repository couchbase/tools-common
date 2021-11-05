package objgcp

import (
	"errors"
	"fmt"
	"path"

	"github.com/couchbase/tools-common/objstore/objerr"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// handleError converts an error relating accessing an object via its key into a user friendly error where possible.
func handleError(bucket, key string, err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, storage.ErrBucketNotExist) {
		// This shouldn't trigger but may aid in debugging in the future
		if bucket == "" {
			bucket = "<empty bucket name>"
		}

		return &objerr.NotFoundError{Type: "bucket", Name: bucket}
	}

	if errors.Is(err, storage.ErrObjectNotExist) {
		// This shouldn't trigger but may aid in debugging in the future
		if key == "" {
			key = "<empty key name>"
		}

		return &objerr.NotFoundError{Type: "key", Name: key}
	}

	return objerr.HandleError(err)
}

// generateKey returns a key which should be used for an in-progress multipart upload. This function should be used to
// generate key names since they'll be prefixed with '<key>-' allowing efficient listing upon completion.
func generateKey(key string) string {
	return path.Join(path.Dir(key), fmt.Sprintf("%s-%s", path.Base(key), uuid.New()))
}
