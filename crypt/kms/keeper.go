package kms

import "context"

type Keeper interface {
	Encrypt(ctx context.Context, plaintext []byte) (ciphertext []byte, err error)
	Decrypt(ctx context.Context, ciphertext []byte) (plaintext []byte, err error)
	Close() error
}
