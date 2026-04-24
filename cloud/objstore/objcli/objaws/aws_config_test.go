package objaws

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
	errutil "github.com/couchbase/tools-common/errors/util"
)

func TestAWSNewSessionOptionsStaticConfig(t *testing.T) {
	expectedClient := &http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConns:        100,
			TLSHandshakeTimeout: time.Minute,
		},
	}

	clientOptions := AWSOptions{
		AccessKeyID:     "not-empty",
		SecretAccessKey: "not-empty",
		Region:          "region",
	}

	config, _, err := AWSNewConfig(context.Background(), clientOptions)
	require.NoError(t, err)

	transport := config.HTTPClient.(*http.Client).Transport.(*http.Transport)
	require.NotNil(t, transport.Proxy)

	// Can't be compared using 'reflect.DeepEqual'
	transport.DialContext = nil
	transport.Proxy = nil

	require.Equal(t, expectedClient, config.HTTPClient)
	require.Equal(t, clientOptions.Region, config.Region)

	creds, err := config.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, clientOptions.AccessKeyID, creds.AccessKeyID)
	require.Equal(t, clientOptions.SecretAccessKey, creds.SecretAccessKey)
}

func TestAWSNewSessionOptionsWithTLSConfig(t *testing.T) {
	expectedClient := &http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http.Transport{
			ForceAttemptHTTP2: true,
			IdleConnTimeout:   90 * time.Second,
			MaxIdleConns:      100,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			TLSHandshakeTimeout: time.Minute,
		},
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	config, _, err := AWSNewConfig(context.Background(), AWSOptions{TLSConfig: tlsConfig})
	require.NoError(t, err)

	transport := config.HTTPClient.(*http.Client).Transport.(*http.Transport)
	require.NotNil(t, transport.Proxy)

	// Can't be compared using 'reflect.DeepEqual'
	transport.DialContext = nil
	transport.Proxy = nil

	require.Equal(t, expectedClient, config.HTTPClient)
}

func TestAWSNewSessionOptionsWithCustomHTTPTimeout(t *testing.T) {
	require.NoError(t, os.Setenv("CB_OBJECT_STORE_HTTP_TIMEOUTS", `{"client":"30s"}`))

	defer func() { require.NoError(t, os.Unsetenv("CB_OBJECT_STORE_HTTP_TIMEOUTS")) }()

	expectedClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConns:        100,
			TLSHandshakeTimeout: time.Minute,
		},
	}

	config, _, err := AWSNewConfig(context.Background(), AWSOptions{})
	require.NoError(t, err)

	transport := config.HTTPClient.(*http.Client).Transport.(*http.Transport)
	require.NotNil(t, transport.Proxy)

	// Can't be compared using 'reflect.DeepEqual'
	transport.DialContext = nil
	transport.Proxy = nil

	require.Equal(t, expectedClient, config.HTTPClient)
}

func TestAWSNewSessionOptionsWithLogLevel(t *testing.T) {
	type test struct {
		input         string
		expected      aws.ClientLogMode
		expectedError bool
	}

	logDebug := aws.LogRequest | aws.LogResponse

	tests := []*test{
		{
			input: "",
		},
		{
			input:    "debug",
			expected: logDebug,
		},
		{
			input:    "debug-with-signing",
			expected: aws.LogSigning | logDebug,
		},
		{
			input:    "debug-with-body",
			expected: aws.LogRequestWithBody | logDebug,
		},
		{
			input:    "debug-with-request-retries",
			expected: aws.LogRetries | logDebug,
		},
		{
			input:    "debug-with-event-stream-body",
			expected: aws.LogRequestEventMessage | logDebug,
		},
		{
			input:         "not-a-log-level",
			expectedError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			options, logger, err := AWSNewConfig(
				context.Background(),
				AWSOptions{
					LogLevel: test.input,
				},
			)

			if test.expectedError {
				require.True(t, errutil.Contains(err, "invalid log level"))
				return
			}

			require.NoError(t, err)
			require.NotNil(t, logger)

			if test.expected != 0 {
				require.Equal(t, test.expected, options.ClientLogMode)
			}
		})
	}
}

func TestAddSchemeIfMissing(t *testing.T) {
	type test struct {
		input    string
		provider objval.Provider
		expected string
	}

	tests := []*test{
		{
			input:    "localhost:1234",
			provider: objval.ProviderAWS,
			expected: "https://localhost:1234",
		},
		{
			input:    "http://localhost:1234",
			provider: objval.ProviderAWS,
			expected: "http://localhost:1234",
		},
		{
			input:    "https://localhost:1234",
			provider: objval.ProviderAWS,
			expected: "https://localhost:1234",
		},
		{
			input:    "localhost:1234",
			provider: objval.ProviderNone,
			expected: "localhost:1234",
		},
		{
			provider: objval.ProviderAWS,
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, AddSchemeIfMissing(test.input, test.provider))
	}
}
