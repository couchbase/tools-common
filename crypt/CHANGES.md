# Changes

## v1.0.0

Initial release.

- Add `crypt.EncryptBytes` and `crypt.DecryptBytes` for AES-256-GCM encryption/decryption.
- Add `crypt/kms.KeyManager` interface and `crypt/kms.CloudKM` implementation supporting AWS KMS, GCP KMS, Azure Key
  Vault, and HashiCorp Vault Transit.
