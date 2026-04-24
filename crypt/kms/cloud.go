package kms

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
)

// CloudKM is used to connect to the 3 major cloud (AWS, GCP, Azure) providers key management solutions.
type CloudKM struct {
	userKey []byte
}

// NewCloudKM will connect to the external KM and load the repository and user keys as needed.
func NewCloudKM(opts *Options) (KeyManager, error) {
	if opts.KeyURL == "" {
		return nil, fmt.Errorf("key url is required")
	}

	keeper, err := openKeeper(opts)
	if err != nil {
		return nil, fmt.Errorf("could not connect to key manager: %w", err)
	}

	defer keeper.Close()

	cloud := &CloudKM{}

	// If there is an encrypted key in the options then decrypt. This will be the case for all commands except config.
	if opts.EncryptedKey != "" {
		cipherText, err := base64.StdEncoding.DecodeString(opts.EncryptedKey)
		if err != nil {
			return nil, fmt.Errorf("invalid encrypted key in repository information: %w", err)
		}

		cloud.userKey, err = keeper.Decrypt(context.Background(), cipherText)
		if err != nil {
			return nil, fmt.Errorf("could not decrypt repository key: %w", err)
		}

		return cloud, nil
	}

	// Generate a 256 bit key.
	cloud.userKey = make([]byte, 32)
	if _, err = rand.Read(cloud.userKey); err != nil {
		return nil, fmt.Errorf("could not generate repository key: %w", err)
	}

	cipherText, err := keeper.Encrypt(context.Background(), cloud.userKey)
	if err != nil {
		return nil, fmt.Errorf("could not encrypt repository key: %w", err)
	}

	opts.EncryptedKey = base64.StdEncoding.EncodeToString(cipherText)

	return cloud, nil
}

func (k *CloudKM) GetRepositoryKey() ([]byte, error) {
	return k.userKey, nil
}

func openKeeper(opts *Options) (Keeper, error) {
	switch {
	case strings.HasPrefix(opts.KeyURL, "awskms://"):
		return getAWSKeeper(strings.TrimPrefix(opts.KeyURL, "awskms://"), opts.KeyRegion,
			opts.SecretAccessKey, opts.AccessKeyID, opts.RefreshToken, opts.OverrideEndpoint)
	case strings.HasPrefix(opts.KeyURL, "gcpkms://"):
		return getGCPKeeper(strings.TrimPrefix(opts.KeyURL, "gcpkms://"), opts.AuthFile)
	case strings.HasPrefix(opts.KeyURL, "azurekeyvault://"):
		return getAzureKeeper(strings.Replace(opts.KeyURL, "azurekeyvault://", "https://", 1),
			opts.TenantID, opts.AccessKeyID, opts.SecretAccessKey)
	case strings.HasPrefix(opts.KeyURL, "hashivault://"),
		strings.HasPrefix(opts.KeyURL, "hashivaults://"):
		return getHashiCorpTransitKeeper(strings.Replace(opts.KeyURL, "hashivault", "http", 1),
			opts.SecretAccessKey)
	default:
		return nil, fmt.Errorf("unknown key url prefix given currently only [awskms://, azurekeyvault://, " +
			"gcpkms://, hashivault:// or hashivaults://] are supported")
	}
}
