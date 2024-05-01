package objaws

import (
	"errors"

	"github.com/aws/smithy-go"

	"github.com/couchbase/tools-common/cloud/v5/objstore/objerr"
	"github.com/couchbase/tools-common/types/ptr"
)

// handleError converts an error relating accessing an object via its key into a user friendly error where possible.
//
// For the full list of error codes supported by AWS S3, please see
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList.
func handleError(bucket, key *string, err error) error {
	errorCode := extractErrorCode(err)
	if errorCode == "" {
		return objerr.HandleError(err)
	}

	switch errorCode {
	case "InvalidAccessKeyId", "SignatureDoesNotMatch":
		return objerr.ErrUnauthenticated
	case "AccessDenied":
		return objerr.ErrUnauthorized
	case "NoSuchKey", "NotFound":
		if key == nil {
			key = ptr.To("<empty key name>")
		}

		return &objerr.NotFoundError{Type: "key", Name: *key}
	case "NoSuchBucket":
		if bucket == nil {
			bucket = ptr.To("<empty bucket name>")
		}

		return &objerr.NotFoundError{Type: "bucket", Name: *bucket}
	}

	// The AWS error type doesn't implement Unwrap, se we must manually unwrap and check it here
	if err := objerr.TryHandleError(errors.Unwrap(err)); err != nil {
		return err
	}

	// This isn't a status code we plan to handle manually, return the complete error
	return err
}

// isKeyNotFound returns a boolean indicating whether the given error is a 'KeyNotFound' error. We also ignore the
// 'NotFound' because localstack returns the wrong error string.
func isKeyNotFound(err error) bool {
	switch extractErrorCode(err) {
	case "NoSuchKey", "NotFound":
		return true
	default:
		return false
	}
}

// isNoSuchUpload returns a boolean indicating whether the given error is an 'NoSuchUpload' error. We also ignore the
// 'NotFound' because localstack returns the wrong error string.
func isNoSuchUpload(err error) bool {
	switch extractErrorCode(err) {
	case "NoSuchUpload", "NotFound":
		return true
	default:
		return false
	}
}

// extractErrorCode returns the error code from the given SDK error.
func extractErrorCode(err error) string {
	var awsErr smithy.APIError

	if err == nil || !errors.As(err, &awsErr) {
		return ""
	}

	return awsErr.ErrorCode()
}
