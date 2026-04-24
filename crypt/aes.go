package crypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

const aesGCMNonceSize = 12

// EncryptBytes encrypts plaintext with AES-256-GCM using dek, returning nonce||ciphertext||tag.
func EncryptBytes(dek, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, aesGCMNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// DecryptBytes decrypts data (nonce||ciphertext||tag) produced by EncryptBytes.
func DecryptBytes(dek, data []byte) ([]byte, error) {
	if len(data) < aesGCMNonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonce, ciphertext := data[:aesGCMNonceSize], data[aesGCMNonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}
