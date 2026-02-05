package cbcrypto

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// WriterOptions holds the configuration for creating a CBCWriter.
type WriterOptions struct {
	// Compression specifies the compression algorithm to use for chunk data.
	Compression CompressionType

	// KeyID is the identifier for the encryption key (max 36 bytes).
	KeyID string

	// Key is the base key. When KeyDerivation is NoDerivation, this must be exactly 32 bytes.
	// When using KBKDF or PBKDF2, this can be any length and will be used to derive the actual encryption key.
	Key []byte

	// KeyDerivation specifies the key derivation method. Defaults to NoDerivation.
	//   - NoDerivation: use Key directly (must be 32 bytes)
	//   - KeyBasedKDF: derive key using KBKDF HMAC/SHA2-256/Counter
	//   - PasswordBasedKDF: derive key using PBKDF2 SHA2-256
	KeyDerivation KeyDerivationMethod

	// PBKDF2IterationExponent determines the number of PBKDF2 iterations when KeyDerivation is PasswordBasedKDF.
	// The iteration count is calculated as: 1024 * 2^PBKDF2IterationExponent
	// Must be in the range [0, 15]. Ignored for other key derivation methods.
	//
	// Examples:
	//   - 0 => 1024 iterations
	//   - 3 => 8192 iterations
	//   - 7 => 131072 iterations
	//
	// When using password-based key derivation, the spec recommends using "password" as the KeyID.
	PBKDF2IterationExponent uint8
}

// CBCWriter manages writing to a cbcrypto encrypted file.
type CBCWriter struct {
	writer   io.Writer
	header   []byte
	headerAD []byte
	offset   int64
	gcm      cipher.AEAD
}

// NewCBCWriter initializes a new encrypted stream, writes the header to the provided writer, and returns a CBCWriter
// for appending data chunks.
//
// A random 16-byte salt is generated for each new CBCwriter. This salt is included in the header, which is used as
// associated data (AD) for all encrypted chunks. When KBKDF or PBKDF2 key derivation is used, the salt is also
// incorporated into the key derivation context.
func NewCBCWriter(w io.Writer, opts WriterOptions) (*CBCWriter, error) {
	if opts.KeyDerivation == PasswordBasedKDF && opts.PBKDF2IterationExponent > 15 {
		return nil, fmt.Errorf("PBKDF2 iteration exponent must be in the range [0, 15], got %d",
			opts.PBKDF2IterationExponent)
	}

	// The header is an 80-byte structure with the following layout:
	//   - Magic          (21 bytes): "\x00Couchbase Encrypted\x00"
	//   - Version        (1 byte)
	//   - Compression    (1 byte)
	//   - Key Derivation (1 byte) (Unused in v0)
	//   - Unused         (3 bytes)
	//   - Key ID Length  (1 byte)
	//   - Key ID         (36 bytes)
	//   - Salt           (16 bytes)
	// The key ID must be 36 bytes. If it's shorter, it will be padded with zeros until it is 36 bytes.
	if len(opts.KeyID) > maxIDLength {
		return nil, fmt.Errorf("key ID cannot be longer than %d bytes", maxIDLength)
	}

	header := make([]byte, headerSize)
	copy(header, magicBytes)
	header[versionOffset] = CurrentVersion
	header[compressionOffset] = byte(opts.Compression)

	// Key derivation byte layout:
	//   - Lower 4 bits: key derivation method (0=none, 1=KBKDF, 2=PBKDF2)
	//   - Upper 4 bits: PBKDF2 iteration exponent (only used when method is PBKDF2)
	//
	// Example: 0x32 means PBKDF2 (method=2) with exponent=3 (8192 iterations).
	keyDerivationMethod := byte(opts.KeyDerivation)
	pbkdf2Exponent := opts.PBKDF2IterationExponent << 4
	header[keyDerivationOffset] = pbkdf2Exponent | keyDerivationMethod

	header[idLenOffset] = byte(len(opts.KeyID))

	paddedKeyID := make([]byte, maxIDLength)
	copy(paddedKeyID, opts.KeyID)
	copy(header[idStartOffset:], paddedKeyID)

	// Generate a random 16-byte salt (stored as a UUID in the header). This salt is used as part of the key derivation
	// context/salt when KBKDF/PBKDF2 is used, and as part of the associated data for all chunks.
	if _, err := io.ReadFull(rand.Reader, header[saltOffset:]); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	// Derive the encryption key based on the key derivation method.
	// We construct a Header here so we can reuse the deriveKey function from reader.go.
	var salt [saltSize]byte

	copy(salt[:], header[saltOffset:saltOffset+saltSize])

	parsedHeader := &Header{
		Version:          CurrentVersion,
		Compression:      opts.Compression,
		KeyDerivation:    opts.KeyDerivation,
		PBKDF2Iterations: uint32(pbkdf2IterationMultiplier * (1 << opts.PBKDF2IterationExponent)),
		KeyID:            opts.KeyID,
		Salt:             salt,
	}

	derivedKey, err := deriveKey(opts.Key, parsedHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to derive encryption key: %w", err)
	}

	if _, err := w.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	headerAD := make([]byte, headerSize+8)
	copy(headerAD, header)

	gcm, err := newGCM(derivedKey)
	if err != nil {
		return nil, err
	}

	return &CBCWriter{
		writer:   w,
		header:   header,
		headerAD: headerAD,
		offset:   headerSize,
		gcm:      gcm,
	}, nil
}

// Open initializes a CBCWriter for an existing cbcrypto file, allowing new chunks to be appended.
//
// The provided 'rws' must be an io.ReadWriteSeeker containing an existing cbcrypto-formatted stream. Upon successful
// return, the seeker will be positioned at the end of the stream, ready for appending.
//
// Open supports both v0 and v1 cbcrypto files. For files with key derivation, the key will be derived automatically
// based on the header's key derivation method.
func Open(rws io.ReadWriteSeeker, baseKey []byte) (*CBCWriter, error) {
	// Ensure we're at the beginning of the file to read the header.
	if _, err := rws.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	headerBytes := make([]byte, headerSize)

	_, err := io.ReadFull(rws, headerBytes)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, &ErrNotEncrypted{Reason: "file is too small to be a valid cbcrypto file"}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	header, err := parseHeader(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse header: %w", err)
	}

	// Derive the encryption key based on the header's key derivation method.
	derivedKey, err := deriveKey(baseKey, header)
	if err != nil {
		return nil, fmt.Errorf("failed to derive encryption key: %w", err)
	}

	gcm, err := newGCM(derivedKey)
	if err != nil {
		return nil, err
	}

	headerAD := make([]byte, headerSize+8)
	copy(headerAD, headerBytes)

	// Seek to the end of the file to get its size and prepare for appending.
	size, err := rws.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return &CBCWriter{
		writer:   rws,
		header:   headerBytes,
		headerAD: headerAD,
		offset:   size,
		gcm:      gcm,
	}, nil
}

// AppendChunk compresses, encrypts, and appends a new data chunk.
func (c *CBCWriter) AppendChunk(data io.Reader) error {
	// Compress the data.
	var compressedData bytes.Buffer
	if err := compressData(&compressedData, data, CompressionType(c.header[compressionOffset])); err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Update headerAD with the new offset.
	binary.BigEndian.PutUint64(c.headerAD[headerSize:], uint64(c.offset))

	// Encrypt the chunk.
	cipherTextWithTag := c.gcm.Seal(nil, nonce, compressedData.Bytes(), c.headerAD)

	// cbcrypto chunk format: 4-byte chunk length | 12-byte nonce | ciphertext | 16-byte GCM tag
	chunkLength := uint32(len(nonce) + len(cipherTextWithTag))
	if _, err := c.writer.Write(binary.BigEndian.AppendUint32(nil, chunkLength)); err != nil {
		return fmt.Errorf("failed to write chunk length: %w", err)
	}

	if _, err := c.writer.Write(nonce); err != nil {
		return fmt.Errorf("failed to write nonce: %w", err)
	}

	if _, err := c.writer.Write(cipherTextWithTag); err != nil {
		return fmt.Errorf("failed to write ciphertext: %w", err)
	}

	c.offset += 4 + int64(chunkLength)

	return nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return gcm, nil
}

// compressData is a helper function that compresses data based on the given compression type.
func compressData(dst io.Writer, src io.Reader, compression CompressionType) error {
	var (
		writer io.WriteCloser
		err    error
	)

	switch compression {
	case None:
		_, err = io.Copy(dst, src)
		if err != nil {
			return fmt.Errorf("failed to write uncompressed data: %w", err)
		}

		return nil
	case Snappy:
		data, err := io.ReadAll(src)
		if err != nil {
			return fmt.Errorf("failed to read data for snappy compression: %w", err)
		}

		if _, err := dst.Write(snappy.Encode(nil, data)); err != nil {
			return fmt.Errorf("failed to write snappy compressed data: %w", err)
		}

		return nil
	case ZLib:
		writer = zlib.NewWriter(dst)
	case GZip:
		writer = gzip.NewWriter(dst)
	case ZStd:
		writer, err = zstd.NewWriter(dst)
		if err != nil {
			return fmt.Errorf("failed to create zstd writer: %w", err)
		}
	case BZip2:
		writer, err = bzip2.NewWriter(dst, nil)
		if err != nil {
			return fmt.Errorf("failed to create bzip2 writer: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression algorithm %d", compression)
	}

	if _, err := io.Copy(writer, src); err != nil {
		return fmt.Errorf("failed to compress with %v: %w", compression, err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close %v writer: %w", compression, err)
	}

	return nil
}
