package objaws

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	httputil "github.com/couchbase/tools-common/http/util"
)

type mockAWSError struct{ inner string }

func (m *mockAWSError) Error() string                 { return m.inner }
func (m *mockAWSError) ErrorCode() string             { return m.inner }
func (m *mockAWSError) ErrorMessage() string          { return m.inner }
func (m *mockAWSError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

func TestNewAWSRetryer(t *testing.T) {
	retryer := NewAWSRetryer()
	require.NotNil(t, retryer)
	require.Equal(t, objcli.DefaultMaxRetries, retryer.MaxAttempts())
}

func TestNewAWSRetryerNonDefaultMaxRetries(t *testing.T) {
	os.Setenv("CB_OBJSTORE_MAX_RETRIES", "42")
	defer os.Unsetenv("CB_OBJSTORE_MAX_RETRIES")

	retryer := NewAWSRetryer()
	require.NotNil(t, retryer)
	require.Equal(t, 42, retryer.MaxAttempts())
}

func TestAWSRetryerRetryShouldRetry(t *testing.T) {
	type test struct {
		name string
		err  error
	}

	tests := []*test{
		{
			name: "ReadConnectionReset",
			err:  &net.OpError{Op: "read", Err: syscall.ECONNRESET},
		},
		{
			name: "DNSError",
			err:  &net.DNSError{},
		},
		{
			name: "UnknownNetworkError",
			err:  net.UnknownNetworkError(""),
		},
		{
			name: "EOF",
			err:  io.EOF,
		},
		{
			name: "UnexpectedEOF",
			err:  io.ErrUnexpectedEOF,
		},
	}

	for _, msg := range httputil.TemporaryErrorMessages {
		tests = append(tests, &test{
			name: msg,
			err: &smithyhttp.ResponseError{
				Err:      fmt.Errorf("asdf%sasdf", msg),
				Response: &smithyhttp.Response{Response: &http.Response{}},
			},
		})
	}

	for k := range awsRetryableErrorCodes {
		tests = append(tests, &test{
			name: k,
			err:  &mockAWSError{inner: k},
		})
	}

	for k := range httputil.TemporaryFailureStatusCodes {
		tests = append(tests, &test{
			name: fmt.Sprintf("StatusCode%d", k),
			err:  &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: k}}},
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !NewAWSRetryer().IsErrorRetryable(test.err) {
				t.Fatalf("Expected true but got false")
			}
		})
	}
}
