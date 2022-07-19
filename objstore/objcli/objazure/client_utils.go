package objazure

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/couchbase/tools-common/objstore/objerr"
)

const (
	azureAccountName    = "accountname"
	azureAccountKey     = "accountkey"
	azureBlobEndpoint   = "blobendpoint"
	azureEndpointSuffix = "endpointsuffix"
)

// GetServiceClient returns the Azure Service Client that facilitates all the necessary interactions with the Azure
// blob storage.
func GetServiceClient(accessKeyID, secretAccessKey, endpoint string, options *azblob.ClientOptions) (
	*azblob.ServiceClient, error,
) {
	serviceURL, err := getServiceURL(endpoint, accessKeyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get service URL: %w", err)
	}

	client, err := getServiceClientWithStaticCredentials(serviceURL, accessKeyID, secretAccessKey, options)
	if err != nil {
		return nil, fmt.Errorf("failed to get service client with static credentials: %w", err)
	}

	if client != nil {
		return client, nil
	}

	client, err = getServiceClientWithTokenCredential(serviceURL, options)
	if err != nil {
		return nil, fmt.Errorf("failed to get service client with token credential: %w", err)
	}

	return client, nil
}

// getServiceClientWithStaticCredentials attempts to create an Azure Service Client with static credentials. In case it
// fails to find any static credentials, instead of failing we proceed to try to create a Service Client with a token
// credential.
func getServiceClientWithStaticCredentials(serviceURL, accessKeyID, secretAccessKey string,
	options *azblob.ClientOptions,
) (*azblob.ServiceClient, error) {
	credentials, err := getStaticCredentials(accessKeyID, secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get static credentials: %w", handleCredsError(err))
	}

	if credentials == nil {
		return nil, nil
	}

	client, err := azblob.NewServiceClientWithSharedKey(serviceURL, credentials, options)
	if err != nil {
		return nil, err // Purposefully not wrapped
	}

	return client, nil
}

// getServiceClientWithStaticCredentials attempts to create an Azure Service Client with a token credential. To achieve
// this we use the Default Azure SDK credential, which tries the following authentication methods:
//   a) Service principal (1. with secret, 2. with certificate, 3. username and password)
//   b) Managed identity
//   c) CLI
// Despite the credential trying all of these methods, we do not support authentication using Service principal with
// username and password, and using CLI.
func getServiceClientWithTokenCredential(serviceURL string, options *azblob.ClientOptions) (*azblob.ServiceClient,
	error,
) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		// We return this error here because this is the last source of credentials that we check
		return nil, objerr.ErrNoCredentialsFound
	}

	client, err := azblob.NewServiceClient(serviceURL, credential, options)
	if err != nil {
		return nil, err // Purposefully not wrapped
	}

	return client, nil
}

// getServiceURL returns the URL which should be used when communicating with the Azure storage service.
func getServiceURL(endpoint, accessKeyID string) (string, error) {
	if endpoint != "" {
		return endpoint, nil
	}

	values := getConnectionStringValues()

	if values != nil && values[azureBlobEndpoint] != "" {
		return values[azureBlobEndpoint], nil
	}

	account, err := azureGetAccount(accessKeyID)
	if err != nil {
		return "", err // Purposefully not wrapped
	}

	suffix := "core.windows.net"
	if values != nil && values[azureEndpointSuffix] != "" {
		suffix = values[azureEndpointSuffix]
	}

	return fmt.Sprintf("https://%s.blob.%s", account, suffix), nil
}

// azureGetAccount returns the account name which should be used when authenticating to Azure, the account name will
// also be used when constructing the endpoint which we use to interact with Azure.
func azureGetAccount(accountName string) (string, error) {
	if accountName != "" {
		return accountName, nil
	}

	if env := os.Getenv("AZURE_STORAGE_ACCOUNT"); env != "" {
		return env, nil
	}

	if values := getConnectionStringValues(); values != nil && values[azureAccountName] != "" {
		return values[azureAccountName], nil
	}

	return "", ErrFailedToDetermineAccountName
}

// getConnectionStringValues reads and parses an Azure style connection string returning a map of the key value pairs.
func getConnectionStringValues() map[string]string {
	cs := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if cs == "" {
		return nil
	}

	values := make(map[string]string)

	for _, kv := range strings.Split(cs, ";") {
		idx := strings.IndexByte(kv, '=')
		if idx <= 0 {
			continue
		}

		values[strings.TrimSpace(strings.ToLower(kv[:idx]))] = strings.TrimSpace(kv[idx+1:])
	}

	return values
}

// getStaticCredentials attempts to create static credentials using the client options or the environment. Returns
// <nil>, <nil> in the event that no static credentials were found.
func getStaticCredentials(accessKeyID, secretAccessKey string) (*azblob.SharedKeyCredential, error) {
	if accessKeyID != "" && secretAccessKey != "" {
		return azblob.NewSharedKeyCredential(accessKeyID, secretAccessKey)
	}

	return getStaticCredentialsFromEnv()
}

// getStaticCredentialsFromEnv attempts to create static credentials using the environment. Returns <nil>, <nil> in the
// event that no static credentials were found.
//
// NOTE: Searches for singular environment variables as well as an Azure style connection string.
func getStaticCredentialsFromEnv() (*azblob.SharedKeyCredential, error) {
	name, key := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_KEY")
	if name != "" && key != "" {
		return azblob.NewSharedKeyCredential(name, key)
	}

	values := getConnectionStringValues()
	if values != nil && values[azureAccountName] != "" && values[azureAccountKey] != "" {
		return azblob.NewSharedKeyCredential(values[azureAccountName], values[azureAccountKey])
	}

	return nil, nil
}

// handleCredsError converts the given error (returned from fetching the credentials) into a more user friendly error
// where possible.
func handleCredsError(err error) error {
	var azureErr *azblob.StorageError
	if err == nil || !errors.As(err, &azureErr) || azureErr.Response() == nil { //nolint:bodyclose
		return err
	}

	switch azureErr.Response().StatusCode { //nolint:bodyclose
	case http.StatusUnauthorized:
		return objerr.ErrUnauthenticated
	case http.StatusForbidden:
		return objerr.ErrUnauthorized
	}

	// This isn't a status code we plan to handle manually, return the complete error
	return err
}
