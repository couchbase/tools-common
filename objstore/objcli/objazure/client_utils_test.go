package objazure

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/objstore/objerr"
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

func TestAzureGetServicePrincipleTokenFromFileEnvNotSet(t *testing.T) {
	os.Unsetenv("AZURE_AUTH_LOCATION")

	actual, err := getServicePrincipleToken()

	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestAzureGetResource(t *testing.T) {
	type test struct {
		name          string
		env           map[string]string
		expected      string
		expectedError bool
	}

	tests := []*test{
		{
			name:     "UseDefaultResource",
			expected: "https://storage.azure.com/",
		},
		{
			name:     "UseAlternativeEnvironment",
			env:      map[string]string{"AZURE_ENVIRONMENT": "AZUREUSGOVERNMENTCLOUD"},
			expected: "https://storage.azure.com/",
		},
		{
			name:     "StaticResource",
			env:      map[string]string{"AZURE_STORAGE_RESOURCE": "resource"},
			expected: "resource",
		},
		{
			name:          "UnknownEnvironment",
			env:           map[string]string{"AZURE_ENVIRONMENT": "unknown"},
			expectedError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, val := range test.env {
				err := os.Setenv(key, val)
				require.NoError(t, err)
				defer os.Unsetenv(key)
			}

			actual, err := getResource()
			require.Equal(t, err != nil, test.expectedError)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestAzureHandleTokenRefreshError(t *testing.T) {
	type test struct {
		name     string
		err      error
		expected string
	}

	tests := []*test{
		{
			name:     "UnknownError",
			err:      errors.New("unknown error"),
			expected: "unknown error",
		},
		{
			name:     "TokenRefreshErrorNilResponse",
			err:      &azureMockTokenRefreshError{msg: "token refresh error"},
			expected: "token refresh error",
		},
		{
			name: "TokenRefreshErrorNotBadRequest",
			err: &azureMockTokenRefreshError{
				msg:  "token refresh error",
				resp: &http.Response{StatusCode: http.StatusBadGateway},
			},
			expected: "token refresh error",
		},
		{
			name: "TokenRefreshErrorNoRegexMatch",
			err: &azureMockTokenRefreshError{
				msg:  "unknown error",
				resp: &http.Response{StatusCode: http.StatusBadRequest},
			},
			expected: "unknown error",
		},
		{
			name: "TokenRefreshErrorEmptyErrorCodes",
			err: &azureMockTokenRefreshError{
				msg:  `Response body: {"error_codes":[]}`,
				resp: &http.Response{StatusCode: http.StatusBadRequest},
			},
			expected: `Response body: {"error_codes":[]}`,
		},
		{
			name: "TokenRefreshErrorNonEmptyErrorCodes",
			err: &azureMockTokenRefreshError{
				msg:  `Response body: {"error_codes":[50001]}`,
				resp: &http.Response{StatusCode: http.StatusBadRequest},
			},
			expected: "the resource is disabled or does not exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := handleTokenRefreshError(test.err)
			require.Equal(t, test.expected, actual.Error())
		})
	}
}

func TestAzureTranslateTokenRefreshErrorCodes(t *testing.T) {
	type test struct {
		name     string
		codes    []uint64
		expected string
	}

	tests := []*test{
		{
			name:  "NotFound",
			codes: []uint64{42},
			expected: fmt.Sprintf("unknown error code(s) '[42]' consult the Azure Active Directory documentation " +
				"for more information"),
		},
		{
			name:     "Found",
			codes:    []uint64{50001},
			expected: "the resource is disabled or does not exist",
		},
		{
			name:     "StableOutput",
			codes:    []uint64{50001, 50010, 50034},
			expected: "audience URI validation for the app failed since no token audiences were configured",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := translateTokenRefreshErrorCodes(test.codes)
			require.Equal(t, test.expected, actual.Error())
		})
	}
}

func TestAzureGetCredentials(t *testing.T) {
	type test struct {
		name            string
		accessKeyID     string
		secretAccessKey string
		env             map[string]string
		expected        azblob.Credential
		expectedError   error
	}

	enc := func(secret string) string {
		return base64.RawStdEncoding.EncodeToString([]byte(secret))
	}

	must := func(account, secret string) azblob.Credential {
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
			name:          "StaticCredentialsMustSupplyBoth",
			accessKeyID:   "account",
			expectedError: objerr.ErrNoValidCredentialsFound,
		},
		{
			name:            "StaticCredentialsMustSupplyBoth",
			secretAccessKey: enc("secret"),
			expectedError:   objerr.ErrNoValidCredentialsFound,
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
			expectedError: objerr.ErrNoValidCredentialsFound,
		},
		{
			name:          "NoValidCredentials",
			expectedError: objerr.ErrNoValidCredentialsFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, val := range test.env {
				err := os.Setenv(key, val)
				require.NoError(t, err)
				defer os.Unsetenv(key)
			}

			actual, err := GetCredentials(test.accessKeyID, test.secretAccessKey)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}

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

			actual, err := getEndpoint(test.endpoint, test.accessKeyID)

			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}
