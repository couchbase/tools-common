package kms

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"

	"github.com/couchbase/tools-common/types/v2/ptr"
)

// azureKeeper implements the Keeper interface for an Azure Key Vault connection.
type azureKeeper struct {
	client     *azkeys.Client
	baseURL    string
	keyName    string
	keyVersion string
}

func (k *azureKeeper) Close() error {
	return nil
}

func (k *azureKeeper) Encrypt(ctx context.Context, plainText []byte) ([]byte, error) {
	options := azkeys.KeyOperationParameters{
		Algorithm: ptr.To(azkeys.EncryptionAlgorithmRSAOAEP256),
		Value:     []byte(base64.URLEncoding.EncodeToString(plainText)),
	}

	res, err := k.client.Encrypt(ctx, k.keyName, k.keyVersion, options, nil)
	if err != nil {
		return nil, fmt.Errorf("could not encrypt data: %w", err)
	}

	if res.Result == nil {
		return nil, fmt.Errorf("empty cipher text returned")
	}

	return res.Result, nil
}

func (k *azureKeeper) Decrypt(ctx context.Context, cipherText []byte) ([]byte, error) {
	options := azkeys.KeyOperationParameters{
		Algorithm: ptr.To(azkeys.EncryptionAlgorithmRSAOAEP256),
		Value:     cipherText,
	}

	res, err := k.client.Decrypt(ctx, k.keyName, k.keyVersion, options, nil)
	if err != nil {
		return nil, fmt.Errorf("could not decrypt data: %w", err)
	}

	if res.Result == nil {
		return nil, fmt.Errorf("empty plain text returned")
	}

	plain, err := base64.URLEncoding.DecodeString(string(res.Result))
	if err != nil {
		return nil, fmt.Errorf("could not base64 decode key: %w", err)
	}

	return plain, nil
}

func getAzureKeeper(url, tenantID, secretID, secretKey string) (Keeper, error) {
	baseURL, keyName, keyVersion, err := parseAzureURL(url)
	if err != nil {
		return nil, err
	}

	creds, err := getCredentials(tenantID, secretID, secretKey)
	if err != nil {
		return nil, fmt.Errorf("could not get credentials for Azure: %w", err)
	}

	client, err := azkeys.NewClient(baseURL, creds, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create an Azure KMS client: %w", err)
	}

	return &azureKeeper{client: client, baseURL: baseURL, keyName: keyName, keyVersion: keyVersion}, nil
}

// keyVaultURL represents the Azure Key Vault key URL. Note that the last match can be <key name>/<version>
var keyVaultURL = regexp.MustCompilePOSIX(`^(https://.+\.vault\.[a-z0-9\-\.]+/)keys/(.+)$`)

func parseAzureURL(url string) (string, string, string, error) {
	matches := keyVaultURL.FindStringSubmatch(url)
	if len(matches) != 3 {
		return "", "", "", fmt.Errorf("invalid Azure Key Vault key URL")
	}

	var (
		keyName    = matches[2]
		keyVersion string
	)

	parts := strings.SplitN(keyName, "/", 2)
	if len(parts) == 2 {
		keyName, keyVersion = parts[0], parts[1]
	}

	return matches[1], keyName, keyVersion, nil
}

// getCredentials will try to get secret credentials if the arguments are all present or will try to get them from the
// environment if none are set. Either all parameters must be passed or none of them, otherwise it will return an error.
func getCredentials(tenantID, secretID, secretKey string) (azcore.TokenCredential, error) {
	if (tenantID != "" || secretID != "" || secretKey != "") && (tenantID == "" || secretID == "" || secretKey == "") {
		return nil, fmt.Errorf(
			"if one of --km-tenant-id, --km-access-key-id, --km-secret-access-key is passed all three must be passed")
	}

	if tenantID == "" {
		return azidentity.NewDefaultAzureCredential(nil)
	}

	return azidentity.NewClientSecretCredential(tenantID, secretID, secretKey, nil)
}
