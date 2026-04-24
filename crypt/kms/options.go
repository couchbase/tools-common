package kms

// Options holds the cloud KMS configuration needed to wrap or unwrap a repository
// data-encryption key. It covers all supported cloud providers (AWS, GCP, Azure,
// HashiCorp Vault) and is intentionally limited to cloud KMS — passphrase mode is
// not represented here.
type Options struct {
	// KeyURL is the URL of the user master key (KEK). The scheme selects the provider.
	KeyURL string
	// EncryptedKey is the wrapped repository DEK, base64-encoded. Read from
	// backup-meta.json's encryption_options.encrypted_key field. When empty,
	// NewCloudKM generates a new DEK and populates this field with the wrapped result.
	EncryptedKey string

	// OverrideEndpoint overrides the KMS endpoint (e.g. for localstack).
	OverrideEndpoint string
	// KeyRegion is the AWS region for awskms://.
	KeyRegion string
	// TenantID is the Azure tenant ID for azurekeyvault://.
	TenantID string
	// AccessKeyID is the AWS access key ID, or Azure client/app ID.
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key, Azure client secret, or Vault token.
	SecretAccessKey string
	// RefreshToken is used by AWS credential providers that issue temporary credentials.
	RefreshToken string
	// AuthFile is the path to a service-account JSON file for gcpkms://.
	AuthFile string
}
