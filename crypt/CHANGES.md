# Changes

## v1.1.0

- Add `crypt/kms.NewCloudKeeper` to connect to a cloud KMS and return a `Keeper` for direct encrypt/decrypt operations
  without managing a repository key.
- Add `JSONCreds` field to `crypt/kms.Options` for passing GCP service-account credentials as raw JSON. Takes
  precedence over `AuthFile` when set.

## v1.0.0

Initial release.

- Add `crypt.EncryptBytes` and `crypt.DecryptBytes` for AES-256-GCM encryption/decryption.
- Add `crypt/kms.KeyManager` interface and `crypt/kms.CloudKM` implementation supporting AWS KMS, GCP KMS, Azure Key
  Vault, and HashiCorp Vault Transit.
