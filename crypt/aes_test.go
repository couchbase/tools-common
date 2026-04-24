package crypt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

var testDEK = []byte{
	0, 1, 2, 3, 4, 5, 6, 7,
	8, 9, 10, 11, 12, 13, 14, 15,
	16, 17, 18, 19, 20, 21, 22, 23,
	24, 25, 26, 27, 28, 29, 30, 31,
}

var testPlaintext = []byte("hello, world — this is a test payload")

func TestEncryptDecryptBytes(t *testing.T) {
	encrypted, err := EncryptBytes(testDEK, testPlaintext)
	require.NoError(t, err)
	require.NotEqual(t, testPlaintext, encrypted)
	require.Greater(t, len(encrypted), aesGCMNonceSize)

	decrypted, err := DecryptBytes(testDEK, encrypted)
	require.NoError(t, err)
	require.True(t, bytes.Equal(testPlaintext, decrypted))
}

func TestEncryptDecryptEmptyPlaintext(t *testing.T) {
	encrypted, err := EncryptBytes(testDEK, []byte{})
	require.NoError(t, err)
	require.Greater(t, len(encrypted), aesGCMNonceSize)

	decrypted, err := DecryptBytes(testDEK, encrypted)
	require.NoError(t, err)
	require.Empty(t, decrypted)
}

func TestEncryptDecryptBinaryPlaintext(t *testing.T) {
	binary := make([]byte, 256)
	for i := range binary {
		binary[i] = byte(i)
	}

	encrypted, err := EncryptBytes(testDEK, binary)
	require.NoError(t, err)

	decrypted, err := DecryptBytes(testDEK, encrypted)
	require.NoError(t, err)
	require.True(t, bytes.Equal(binary, decrypted))
}

func TestEncryptBytesRandomNonce(t *testing.T) {
	dek := make([]byte, 32)

	enc1, err := EncryptBytes(dek, []byte("data"))
	require.NoError(t, err)

	enc2, err := EncryptBytes(dek, []byte("data"))
	require.NoError(t, err)

	require.NotEqual(t, enc1, enc2)
}

func TestEncryptBytesInvalidKeySize(t *testing.T) {
	_, err := EncryptBytes([]byte("tooshort"), testPlaintext)
	require.ErrorContains(t, err, "create AES cipher")
}

func TestDecryptBytesInvalidKeySize(t *testing.T) {
	enc, err := EncryptBytes(testDEK, testPlaintext)
	require.NoError(t, err)

	_, err = DecryptBytes([]byte("tooshort"), enc)
	require.ErrorContains(t, err, "create AES cipher")
}

func TestDecryptBytesWrongKey(t *testing.T) {
	enc, err := EncryptBytes(testDEK, []byte("secret"))
	require.NoError(t, err)

	wrongDEK := bytes.Clone(testDEK)
	wrongDEK[0] ^= 1

	_, err = DecryptBytes(wrongDEK, enc)
	require.ErrorContains(t, err, "decrypt")
}

func TestDecryptBytesTamperedCiphertext(t *testing.T) {
	enc, err := EncryptBytes(testDEK, testPlaintext)
	require.NoError(t, err)

	enc[aesGCMNonceSize] ^= 0xFF

	_, err = DecryptBytes(testDEK, enc)
	require.ErrorContains(t, err, "decrypt")
}

func TestDecryptBytesTamperedNonce(t *testing.T) {
	enc, err := EncryptBytes(testDEK, testPlaintext)
	require.NoError(t, err)

	enc[0] ^= 0xFF

	_, err = DecryptBytes(testDEK, enc)
	require.ErrorContains(t, err, "decrypt")
}

func TestDecryptBytesTooShort(t *testing.T) {
	_, err := DecryptBytes(testDEK, make([]byte, aesGCMNonceSize-1))
	require.ErrorContains(t, err, "ciphertext too short")
}

func TestDecryptBytesExactNonceLength(t *testing.T) {
	// aesGCMNonceSize bytes passes the length check but has no ciphertext+tag, so GCM open must fail.
	_, err := DecryptBytes(testDEK, make([]byte, aesGCMNonceSize))
	require.ErrorContains(t, err, "decrypt")
}
