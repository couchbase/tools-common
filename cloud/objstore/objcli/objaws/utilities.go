package objaws

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/couchbase/tools-common/cloud/v2/objstore/objerr"
	"github.com/couchbase/tools-common/types/ptr"
)

// handleError converts an error relating accessing an object via its key into a user friendly error where possible.
func handleError(bucket, key *string, err error) error {
	var awsErr awserr.Error
	if err == nil || !errors.As(err, &awsErr) {
		return objerr.HandleError(err)
	}

	switch awsErr.Code() {
	case "InvalidAccessKeyId", "SignatureDoesNotMatch":
		return objerr.ErrUnauthenticated
	case "AccessDenied":
		return objerr.ErrUnauthorized
	case s3.ErrCodeNoSuchKey, sns.ErrCodeNotFoundException:
		if key == nil {
			key = ptr.To("<empty key name>")
		}

		return &objerr.NotFoundError{Type: "key", Name: *key}
	case s3.ErrCodeNoSuchBucket:
		if bucket == nil {
			bucket = ptr.To("<empty bucket name>")
		}

		return &objerr.NotFoundError{Type: "bucket", Name: *bucket}
	case aws.ErrMissingEndpoint.Code():
		return objerr.ErrEndpointResolutionFailed
	}

	// The AWS error type doesn't implement Unwrap, se we must manually unwrap and check it here
	if err := objerr.TryHandleError(awsErr.OrigErr()); err != nil {
		return err
	}

	// This isn't a status code we plan to handle manually, return the complete error
	return err
}

// isKeyNotFound returns a boolean indicating whether the given error is a 'KeyNotFound' error. We also ignore the 'sns'
// not found exception because localstack returns the wrong error string ('NotFound').
func isKeyNotFound(err error) bool {
	var awsErr awserr.Error
	return errors.As(err, &awsErr) &&
		(awsErr.Code() == sns.ErrCodeNotFoundException || awsErr.Code() == s3.ErrCodeNoSuchKey)
}

// isNoSuchUpload returns a boolean indicating whether the given error is an 'NoSuchUpload' error. We also ignore the
// 'sns' not found exception because localstack returns the wrong error string ('NotFound').
func isNoSuchUpload(err error) bool {
	var awsErr awserr.Error
	return errors.As(err, &awsErr) && (awsErr.Code() == sns.ErrCodeNotFoundException ||
		awsErr.Code() == s3.ErrCodeNoSuchUpload)
}
