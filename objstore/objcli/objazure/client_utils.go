package objazure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/couchbase/clog"

	"github.com/couchbase/tools-common/objstore/objerr"
)

const (
	azureAccountName    = "accountname"
	azureAccountKey     = "accountkey"
	azureBlobEndpoint   = "blobendpoint"
	azureEndpointSuffix = "endpointsuffix"
)

// GetCredentials returns the credentials which should be used to authenticate against Azure.
//
// NOTE: This function acts as a wrapper around getCredentials() and converts any returned errors into more
// client-appropriate ones whenever possible.
func GetCredentials(accessKeyID, secretAccessKey string) (azblob.Credential, error) {
	credentials, err := getCredentials(accessKeyID, secretAccessKey)
	if err != nil {
		return nil, handleCredsError(err)
	}

	return credentials, nil
}

// GetURL returns the URL that should be used when communicating with the Azure blob service.
func GetURL(endpoint, accessKeyID string) (*url.URL, error) {
	endpoint, err := getEndpoint(endpoint, accessKeyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoint: %w", err)
	}

	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint as URL: %w", err)
	}

	return url, nil
}

// NewHTTPClientFactory creates a new HTTP client factory which will be used by the SDK when sending HTTP requests.
func NewHTTPClientFactory(client *http.Client) pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			resp, err := client.Do(request.WithContext(ctx)) //nolint:bodyclose
			if err != nil {
				err = pipeline.NewError(err, "HTTP request failed")
			}

			return pipeline.NewHTTPResponse(resp), err
		}
	})
}

// getEndpoint returns the endpoint which should be used when communicating with the Azure blob service.
func getEndpoint(endpoint, accessKeyID string) (string, error) {
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

// getCredentials returns the credentials which should be used to authenticate against Azure.
func getCredentials(accessKeyID, secretAccessKey string) (azblob.Credential, error) {
	credentials, err := getStaticCredentials(accessKeyID, secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get static credentials: %w", err)
	}

	if credentials != nil {
		return credentials, nil
	}

	credentials, err = getTokenCredential()
	if err != nil {
		return nil, fmt.Errorf("failed to get principle token credentials: %w", err)
	}

	if credentials != nil {
		return credentials, nil
	}

	return nil, objerr.ErrNoValidCredentialsFound
}

// getStaticCredentials attempts to create static credentials using the client options, environment. Returns <nil>,
// <nil> in the event that no static credentials were found.
func getStaticCredentials(accessKeyID, secretAccessKey string) (azblob.Credential, error) {
	if accessKeyID != "" && secretAccessKey != "" {
		return azblob.NewSharedKeyCredential(accessKeyID, secretAccessKey)
	}

	return getStaticCredentialsFromEnv()
}

// getStaticCredentialsFromEnv attempts to create static credentials using the environment. Returns <nil>, <nil> in the
// event that no static credentials were found.
//
// NOTE: Searches for singular environment variables as well as an Azure style connection string.
func getStaticCredentialsFromEnv() (azblob.Credential, error) {
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

// getTokenCredential attempts to create token credential using a service principle token. Returns <nil>, <nil> in the
// event that no valid token could be generated.
func getTokenCredential() (azblob.Credential, error) {
	spt, err := getServicePrincipleToken()
	if err != nil {
		return nil, fmt.Errorf("failed to get service principle token: %w", err)
	}

	if spt == nil {
		return nil, nil
	}

	// We refresh the token prior to creating our 'refresher' below; this allows us to catch authentication errors early
	// and return a useful error message to the user.
	err = spt.Refresh()
	if err != nil {
		return nil, handleTokenRefreshError(err)
	}

	// refresher is called by the SDK to refresh the service principle token after a given timeout.
	//
	// NOTE: Can return a zero value duration to stop refreshes in the event of a failure, for the time being we'll just
	// allow requests to fail and we'll exit early.
	refresher := func(credential azblob.TokenCredential) time.Duration {
		if err := spt.Refresh(); err != nil {
			clog.Errorf("Failed to refresh service principle token: %s", handleTokenRefreshError(err))
		}

		token := spt.Token()
		credential.SetToken(token.AccessToken)

		return token.Expires().Sub(time.Now().Add(time.Minute))
	}

	return azblob.NewTokenCredential(spt.Token().AccessToken, refresher), nil
}

// getServicePrincipleToken attempts to retrieve a service principle token via the environment or using the users
// Azure configuration file. Returns <nil>, <nil> in the event that no valid token could be generated.
func getServicePrincipleToken() (*adal.ServicePrincipalToken, error) {
	spt, err := getServicePrincipleTokenFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to get token from environment: %w", err)
	}

	if spt != nil {
		return spt, nil
	}

	spt, err = getServicePrincipleTokenFromFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get token from file: %w", err)
	}

	return spt, nil
}

// getServicePrincipleTokenFromEnvironment attempts to create a service principle token using the current
// environment. Returns <nil>, <nil> in the event that no valid token could be generated.
func getServicePrincipleTokenFromEnvironment() (*adal.ServicePrincipalToken, error) {
	settings, err := auth.GetSettingsFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to get settings environment: %w", err)
	}

	settings.Values[auth.Resource], err = getResource()
	if err != nil {
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}

	type tokenizer interface {
		ServicePrincipalToken() (*adal.ServicePrincipalToken, error)
	}

	token := func(t tokenizer, err error) *adal.ServicePrincipalToken {
		if err != nil {
			return nil
		}

		spt, err := t.ServicePrincipalToken()
		if err != nil {
			return nil
		}

		return spt
	}

	if spt := token(settings.GetClientCredentials()); spt != nil {
		return spt, nil
	}

	if spt := token(settings.GetClientCertificate()); spt != nil {
		return spt, nil
	}

	if spt := token(settings.GetUsernamePassword()); spt != nil {
		return spt, nil
	}

	if !adal.MSIAvailable(context.Background(), nil) {
		return nil, nil
	}

	msi := auth.MSIConfig{
		ClientID: settings.Values[auth.ClientID],
		Resource: settings.Environment.ResourceIdentifiers.Storage,
	}

	if spt := token(msi, nil); spt != nil {
		return spt, nil
	}

	return nil, nil
}

// getServicePrincipleTokenFromFile attempts to create a service principle token using an Azure SDK style
// authentication file. Returns <nil>, <nil> in the event that no valid token could be generated.
func getServicePrincipleTokenFromFile() (*adal.ServicePrincipalToken, error) {
	path := os.Getenv("AZURE_AUTH_LOCATION")

	// The user hasn't got an azure auth location set, don't bother trying this method
	if path == "" {
		return nil, nil
	}

	settings, err := auth.GetSettingsFromFile()
	if err != nil {
		return nil, fmt.Errorf("failed to read auth settings from file at '%s': %w", path, err)
	}

	resource, err := getResource()
	if err != nil {
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}

	spt, err := settings.ServicePrincipalTokenFromClientCredentialsWithResource(resource)
	if err == nil {
		return spt, nil
	}

	spt, err = settings.ServicePrincipalTokenFromClientCertificateWithResource(resource)
	if err == nil {
		return spt, nil
	}

	return nil, nil
}

// getResource returns a string representation of the resource which should be used when attempting to create a
// service principle token.
//
// We are following a similar setup to that displayed in the 'azure-sdk-for-go'. The main difference is that we are not
// creating 'go-autorest' authorizers; we're creating auth mechanisms which are valid for 'azure-storage-blob-go'.
//
// See https://github.com/Azure/azure-sdk-for-go/blob/master/services/keyvault/auth/auth.go for more information.
func getResource() (string, error) {
	environment, err := getEnvironment()
	if err != nil {
		return "", err
	}

	resource := os.Getenv("AZURE_STORAGE_RESOURCE")
	if resource == "" {
		resource = environment.ResourceIdentifiers.Storage
	}

	return resource, nil
}

// getEnvironment determines which Azure environment is being used, this is done by checking the 'AZURE_ENVIRONMENT'
// environment variable.
//
// NOTE: Returns the default public cloud if 'AZURE_ENVIRONMENT' is not set.
func getEnvironment() (azure.Environment, error) {
	val := os.Getenv("AZURE_ENVIRONMENT")
	if val == "" {
		return azure.PublicCloud, nil
	}

	environment, err := azure.EnvironmentFromName(val)
	if err != nil {
		return azure.Environment{}, err
	}

	return environment, nil
}

// handleTokenRefreshError handles the error returned when attempting to refresh the ADAL token, it does so by
// extracting the error code(s) and returning a more user friendly error message.
func handleTokenRefreshError(err error) error {
	var refreshErr adal.TokenRefreshError
	if !errors.As(err, &refreshErr) {
		return err
	}

	resp := refreshErr.Response() //nolint:bodyclose
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		return err
	}

	// This is a really nasty hack and what happens when you don't return proper error messages; for the time being, we
	// will just extract the JSON payload from the error message.
	match := regexp.MustCompile(`Response body: ({.*})`).FindStringSubmatch(err.Error())
	if match == nil || len(match) != 2 {
		return err
	}

	type overlay struct {
		ErrorCodes []uint64 `json:"error_codes"`
	}

	var decoded overlay
	if err := json.Unmarshal([]byte(match[1]), &decoded); err == nil && len(decoded.ErrorCodes) > 0 {
		return translateTokenRefreshErrorCodes(decoded.ErrorCodes)
	}

	return err
}

// translateTokenRefreshErrorCodes converts the provided code(s) into a user friendly error message.
func translateTokenRefreshErrorCodes(codes []uint64) error {
	conv := map[uint64]string{
		50001:   "the resource is disabled or does not exist",
		50010:   "audience URI validation for the app failed since no token audiences were configured",
		50034:   "user account not found",
		50057:   "user account is disabled",
		50064:   "credential validation on username or password has failed",
		50126:   "error validating credentials due to invalid username or password",
		50135:   "password change is required due to account risk",
		50197:   "The user could not be found",
		51004:   "the user account doesn’t exist in the directory",
		53003:   "access has been blocked by Conditional Access policies",
		70001:   "the application is disabled",
		90002:   "The tenant name wasn't found in the data store, check to make sure you have the correct tenant ID",
		90051:   "the national cloud identifier contains an invalid cloud identifier",
		90094:   "administrator consent is required",
		90099:   "the application has not been authorized in the tenant",
		700016:  "the application wasn't found in the directory/tenant",
		7000112: "the application is disabled",
		7000114: "application is not allowed to make application on-behalf-of calls",
		7000215: "invalid client secret is provided",
	}

	msgs := make([]string, 0, 1)

	for _, code := range codes {
		msg, ok := conv[code]
		if !ok {
			continue
		}

		msgs = append(msgs, msg)
	}

	if len(msgs) == 0 {
		return fmt.Errorf("unknown error code(s) '%v' consult the Azure Active Directory documentation for "+
			"more information", codes)
	}

	sort.Strings(msgs)

	return errors.New(msgs[0])
}

// handleCredsError converts the given error (returned from fetching the credentials) into a more user friendly error
// where possible.
func handleCredsError(err error) error {
	var azureErr azblob.StorageError
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
