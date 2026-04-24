package objaws

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	httputil "github.com/couchbase/tools-common/http/util"
)

// awsRetryableErrorCodes - A map which contains known error codes which can/should be retried by default.
var awsRetryableErrorCodes = map[string]struct{}{
	// The Content-MD5 you specified did not match that received.
	"BadDigest": {},
	// The service is unavailable. Please retry.
	"Busy": {},
	// You did not provide the number of bytes specified by the Content-Length HTTP header.
	"IncompleteBody": {},
	// The server cannot process the request because there is not enough space on disk.
	"InsufficientStorage": {},
	// An internal error was encountered. Please try again.
	"InternalError": {},
	// A conflicting conditional operation is currently in progress against this resource. Please try again.
	"OperationAborted": {},
	// The difference between the request time and the server's time is too large.
	"RequestTimeTooSkewed": {},
	// An internal timeout error was encountered. Please try again.
	"ServerTimeout": {},
	// Please reduce your request rate.
	"ServiceUnavailable": {},
	// Please reduce your request rate.
	"SlowDown": {},
	// The provided token must be refreshed.
	"TokenRefreshRequired": {},
	// The provided token has expired.
	"ExpiredToken": {},
	// The Content-SHA256 you specified did not match what we received
	"XAmzContentSHA256Mismatch": {},
}

// AWSRetryer - Extends the 'DefaultRetryer' provided by the AWS SDK to retry some additional errors which users are
// expected to hit in 'cbbackupmgr'.
type AWSRetryer struct {
	inner *retry.Standard
}

var _ aws.Retryer = (*AWSRetryer)(nil)

// NewAWSRetryer - Returns a new AWS retryer with a user configurable number of retries.
func NewAWSRetryer() *AWSRetryer {
	maxBackoff := objcli.GetMaxRetryBackoff()

	return &AWSRetryer{inner: retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = objcli.GetMaxRetries()
		o.MaxBackoff = maxBackoff
		o.Backoff = retry.NewExponentialJitterBackoff(maxBackoff)

		// We turn off client-side rate limiting because we want to retry as many times as we have configured to
		// do so, regardless of how many other requests we have made. See MB-63728 for details.
		o.RateLimiter = ratelimit.None
	})}
}

func (a *AWSRetryer) IsErrorRetryable(err error) bool {
	if err == nil {
		return false
	}

	return httputil.IsTemporaryError(err) || a.shouldRetryCode(err) || a.shouldRetryFromResponse(err) ||
		a.inner.IsErrorRetryable(err) || errors.Is(err, io.EOF)
}

// MaxAttempts returns the maximum number of attempts that can be made for
// an attempt before failing. A value of 0 implies that the attempt should
// be retried until it succeeds if the errors are retryable.
func (a *AWSRetryer) MaxAttempts() int { return a.inner.MaxAttempts() }

// RetryDelay returns the delay that should be used before retrying the
// attempt. Will return error if the delay could not be determined.
func (a *AWSRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	return a.inner.RetryDelay(attempt, opErr)
}

// GetRetryToken attempts to deduct the retry cost from the retry token pool.
// Returning the token release function, or error.
func (a *AWSRetryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return a.inner.GetRetryToken(ctx, opErr)
}

// GetInitialToken returns the initial attempt token that can increment the
// retry token pool if the attempt is successful.
func (a *AWSRetryer) GetInitialToken() (releaseToken func(error) error) {
	return a.inner.GetInitialToken() //nolint:staticcheck
}

// shouldRetryCode - Returns a boolean indicating whether the provided error should be retried. This is determined by
// extracting and inspecting the AWS error code.
func (a *AWSRetryer) shouldRetryCode(err error) bool {
	var awsErr smithy.APIError

	if !errors.As(err, &awsErr) {
		return false
	}

	_, ok := awsRetryableErrorCodes[awsErr.ErrorCode()]

	return ok
}

// shouldRetryFromResponse - Returns a boolean indicating whether the provided error should be retried due to the HTTP
// status code or response error; this is done by treating it as a 'ResponseError'.
func (a *AWSRetryer) shouldRetryFromResponse(err error) bool {
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}

	return httputil.IsTemporaryFailure(respErr.HTTPStatusCode()) || httputil.IsTemporaryError(respErr.Err)
}
