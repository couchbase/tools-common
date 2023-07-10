package objazure

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/couchbase/tools-common/cloud/objstore/objerr"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type azureMockTokenRefreshError struct {
	msg  string
	resp *http.Response
}

func (a *azureMockTokenRefreshError) Error() string {
	return a.msg
}

func (a *azureMockTokenRefreshError) Response() *http.Response {
	return a.resp
}

var _ adal.TokenRefreshError = (*azureMockTokenRefreshError)(nil)

func TestAzureGetStaticCredentials(t *testing.T) {
	type test struct {
		name            string
		accessKeyID     string
		secretAccessKey string
		env             map[string]string
		expected        *azblob.SharedKeyCredential
	}

	enc := func(secret string) string {
		return base64.RawStdEncoding.EncodeToString([]byte(secret))
	}

	must := func(account, secret string) *azblob.SharedKeyCredential {
		credentials, err := azblob.NewSharedKeyCredential(account, enc(secret))
		require.NoError(t, err)

		return credentials
	}

	tests := []*test{
		{
			name:            "StaticCredentials",
			accessKeyID:     "account",
			secretAccessKey: enc("secret"),
			// Static credentials should take priority
			env:      map[string]string{"AZURE_STORAGE_ACCOUNT": "another", "AZURE_STORAGE_KEY": enc("secret")},
			expected: must("account", "secret"),
		},
		{
			name:        "StaticCredentialsMustSupplyBoth",
			accessKeyID: "account",
		},
		{
			name:            "StaticCredentialsMustSupplyBoth",
			secretAccessKey: enc("secret"),
		},
		{
			name: "StaticCredentialsViaEnv",
			env: map[string]string{
				"AZURE_STORAGE_ACCOUNT": "account", "AZURE_STORAGE_KEY": enc("secret"),
				// Static env should take priority over a connection string
				"AZURE_STORAGE_CONNECTION_STRING": fmt.Sprintf("AccountName=another;AccountKey=%s", enc("secret")),
			},
			expected: must("account", "secret"),
		},
		{
			name: "StaticCredentialsViaConnectionString",
			env: map[string]string{
				"AZURE_STORAGE_CONNECTION_STRING": fmt.Sprintf("AccountName=account;AccountKey=%s", enc("secret")),
			},
			expected: must("account", "secret"),
		},
		{
			name: "MalformedConnectionString",
			env: map[string]string{
				"AZURE_STORAGE_CONNECTION_STRING": fmt.Sprintf("AccountNameaccount;AccountKey=%s", enc("secret")),
			},
		},
		{
			name: "NoValidCredentials",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, val := range test.env {
				err := os.Setenv(key, val)
				require.NoError(t, err)
				defer os.Unsetenv(key)
			}

			actual, err := getStaticCredentials(test.accessKeyID, test.secretAccessKey)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestAzureGetConnectionStringValues(t *testing.T) {
	type test struct {
		name     string
		cstr     string
		expected map[string]string
	}

	tests := []*test{
		{
			name: "EmptyConnectionString",
		},
		{
			name:     "SingleKeyConnectionString",
			cstr:     "AccountName=name",
			expected: map[string]string{"accountname": "name"},
		},
		{
			name:     "MultiKeyConnectionString",
			cstr:     "AccountName=name;AccountKey=key",
			expected: map[string]string{"accountname": "name", "accountkey": "key"},
		},
		{
			name:     "MalformedConnectionStringPair",
			cstr:     "AccountNamename;AccountKey=key",
			expected: map[string]string{"accountkey": "key"},
		},
		{
			// It's up to the user to provide a valid connection string, an invalid connection string will likely result
			// in useless key/value pairs. Note that the Go SDK doesn't perform validation either.
			name:     "InvalidConnectionString",
			cstr:     "AccountName=name,AccountKey=key",
			expected: map[string]string{"accountname": "name,AccountKey=key"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			os.Setenv("AZURE_STORAGE_CONNECTION_STRING", test.cstr)
			defer os.Unsetenv("AZURE_STORAGE_CONNECTION_STRING")

			require.Equal(t, test.expected, getConnectionStringValues())
		})
	}
}

func TestAzureGetEndpoint(t *testing.T) {
	type test struct {
		name          string
		endpoint      string
		accessKeyID   string
		env           map[string]string
		expected      string
		expectedError error
	}

	tests := []*test{
		{
			name:     "StaticEndpoint",
			endpoint: "endpoint",
			expected: "endpoint",
		},
		{
			name:        "AccountViaStatic",
			accessKeyID: "account",
			env:         map[string]string{"AZURE_STORAGE_ACCOUNT": "another_account"}, // Static credential should take priority
			expected:    "https://account.blob.core.windows.net",
		},
		{
			name: "AccountViaEnv",
			env: map[string]string{
				"AZURE_STORAGE_ACCOUNT":           "account",
				"AZURE_STORAGE_CONNECTION_STRING": "AccountName=another_account", // Static env should take priority
			},
			expected: "https://account.blob.core.windows.net",
		},
		{
			name:     "AccountViaConnectionString",
			env:      map[string]string{"AZURE_STORAGE_CONNECTION_STRING": "AccountName=account"},
			expected: "https://account.blob.core.windows.net",
		},
		{
			name:          "NoAccount",
			expectedError: ErrFailedToDetermineAccountName,
		},
		{
			name:     "OverrideSuffixViaConnectionString",
			env:      map[string]string{"AZURE_STORAGE_CONNECTION_STRING": "AccountName=account;EndpointSuffix=suffix"},
			expected: "https://account.blob.suffix",
		},
		{
			name:     "EmptyOverrideSuffixViaConnectionString",
			env:      map[string]string{"AZURE_STORAGE_CONNECTION_STRING": "AccountName=account;EndpointSuffix="},
			expected: "https://account.blob.core.windows.net",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, val := range test.env {
				err := os.Setenv(key, val)
				require.NoError(t, err)
				defer os.Unsetenv(key)
			}

			actual, err := getServiceURL(test.endpoint, test.accessKeyID)

			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestHandleCredsError(t *testing.T) {
	type test struct {
		name     string
		err      error
		expected error
	}

	tests := []*test{
		{
			name: "Nil",
		},
		{
			name:     "UnknownError",
			err:      assert.AnError,
			expected: assert.AnError,
		},
		{
			name:     "Unauthenticated",
			err:      respError(bloberror.AuthenticationFailed),
			expected: objerr.ErrUnauthenticated,
		},
		{
			name:     "Unauthorized",
			err:      respError(bloberror.AuthorizationFailure),
			expected: objerr.ErrUnauthorized,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, handleCredsError(test.err), test.expected)
		})
	}
}
