package cbcrypto

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	headerSize     = 80
	versionOffset  = 21
	compressionOff = 22
	idLenOffset    = 27
	idStartOffset  = 28
	saltOffset     = 64

	maxIDLength = 36
	minChunkLen = 12 + 16 // per cbcrypto spec: nonce (12 bytes) + GCM tag (16 bytes)
)

// magicBytes is the cbcrypto file format magic string: "\x00Couchbase Encrypted\x00"
var magicBytes = []byte("\x00Couchbase Encrypted\x00")

// CompressionType defines the compression algorithm used in an encrypted file.
type CompressionType int

const (
	// These values are based on the cbcrypto encrypted file format specification.
	None CompressionType = iota
	Snappy
	ZLib
	GZip
	ZStd
	BZip2
)

// KeyProvider is a function that returns a data encryption key for a given key ID.
type KeyProvider func(keyID string) ([]byte, error)

var _ io.Reader = (*Reader)(nil)

// Reader decrypts a cbcrypto stream on-demand.
type Reader struct {
	// chunkSource provides encrypted chunk bytes, positioned immediately after the header.
	chunkSource io.Reader

	// chunkAEAD decrypts each chunk using the DEK recovered from the header.
	chunkAEAD cipher.AEAD

	// compressionMode is the algorithm recorded in the file header to decompress chunk plaintext.
	compressionMode CompressionType

	// nextChunkOffset tracks the absolute file position of the next chunk length field (needed for AD).
	nextChunkOffset int64

	// headerAD is the reusable buffer `header || uint64(offset)`. The final 8 bytes are overwritten with the
	// big-endian chunk offset immediately before each AES-GCM Open call.
	headerAD []byte

	// Reusable buffers to reduce allocations when processing chunks.
	encryptedBuf []byte
	decryptedBuf []byte

	// chunkReader streams from the current decrypted + decompressed chunk.
	chunkReader io.ReadCloser

	finished bool
}

// NewReader returns a Reader that decrypts and decompresses a cbcrypto stream on demand.
func NewReader(r io.Reader, provider KeyProvider) (*Reader, error) {
	// Layout overview (see 'cbcrypto/EncryptedFileFormat.md' in the 'platform' repo for the complete specification):
	//   • 80-byte header   – "\0Couchbase Encrypted\0" magic, version, compression, key-id, salt.
	//   • One or more chunks, each encoded as:
	//       4-byte length (network order)
	//       12-byte nonce | ciphertext | 16-byte GCM tag
	//
	// Associated-data (AD): for every chunk the entire 80-byte header is concatenated with the 64-bit file offset of
	// the chunk length field and passed as AD to AES-256-GCM.
	// In pseudocode: AD = header || uint64(offset)
	//
	// After decrypting all chunks the resulting stream is optionally decompressed (Snappy / zlib / gzip / bzip2) to
	// obtain the original payload.
	headerData := make([]byte, headerSize)
	if _, err := io.ReadFull(r, headerData); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	header, err := parseHeader(headerData)
	if err != nil {
		return nil, err
	}

	// Obtain the key using the provided KeyProvider, and create an AES cipher.
	dek, err := provider(header.KeyID)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain data-encryption-key for %q: %w", header.KeyID, err)
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	headerAD := make([]byte, headerSize+8)
	copy(headerAD, headerData)

	return &Reader{
		chunkSource:     r,
		chunkAEAD:       gcm,
		compressionMode: header.Compression,
		nextChunkOffset: headerSize,
		headerAD:        headerAD,
	}, nil
}

// Read implements io.Reader by streaming decrypted plaintext from chunkSource one chunk at a time.
//
// It will read until either:
//
// 1. The destination buffer 'p' is filled.
//
// 2. The current chunk is fully read.
//
// 3. EOF is reached.
//
// NOTE: since at most one chunk is read at a time, many calls to Read may result in relatively small reads.
func (c *Reader) Read(p []byte) (int, error) {
	if c.finished {
		return 0, io.EOF
	}

	if len(p) == 0 {
		return 0, nil
	}

	// No chunk reader means we aren't currently "on" a chunk, so we need to load the next one.
	if c.chunkReader == nil {
		err := c.loadNextChunk()
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.finished = true
			}

			return 0, err
		}
	}

	n, err := c.chunkReader.Read(p)

	// An EOF here just means we've reached the end of the current chunk, not necessarily the end of the file.
	// So we don't return an error, but we do close chunkReader and set it to nil, so on the next call to Read, we'll
	// load the next chunk.
	if errors.Is(err, io.EOF) {
		if closeErr := c.chunkReader.Close(); closeErr != nil {
			return n, fmt.Errorf("failed to close chunk reader: %w", closeErr)
		}

		c.chunkReader = nil
	}

	return n, nil
}

func (c *Reader) loadNextChunk() error {
	// cbcrypto chunk format: 4-byte chunk length | 12-byte nonce | ciphertext | 16-byte GCM tag
	var chunkLen uint32

	err := binary.Read(c.chunkSource, binary.BigEndian, &chunkLen)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		return fmt.Errorf("failed to read chunk length: %w", err)
	}

	if int(chunkLen) < minChunkLen {
		return fmt.Errorf("chunk at offset %d has size %d which is less than minimum required size %d",
			c.nextChunkOffset, int(chunkLen), minChunkLen)
	}

	// We reuse the same buffer for each chunk to reduce allocations.
	// We grow the buffer if the next chunk is larger than the current buffer.
	if cap(c.encryptedBuf) < int(chunkLen) {
		c.encryptedBuf = make([]byte, int(chunkLen))
	} else {
		c.encryptedBuf = c.encryptedBuf[:int(chunkLen)]
	}

	if _, err := io.ReadFull(c.chunkSource, c.encryptedBuf); err != nil {
		return fmt.Errorf("failed to read chunk at offset %d: %w", c.nextChunkOffset, err)
	}

	var (
		encryptedChunk    = c.encryptedBuf
		chunkStartOffset  = c.nextChunkOffset
		nonce             = encryptedChunk[:12]
		cipherTextWithTag = encryptedChunk[12:]
	)

	c.nextChunkOffset += int64(4 + len(encryptedChunk))

	// Build AD = header || uint64(chunk offset). Overwrite the last 8 bytes with the new chunk start offset.
	binary.BigEndian.PutUint64(c.headerAD[len(c.headerAD)-8:], uint64(chunkStartOffset))

	// Decrypt in-place into c.decryptedBuf to avoid allocations. The Open method will grow c.decryptedBuf if
	// necessary.
	plainChunk, err := c.chunkAEAD.Open(c.decryptedBuf[:0], nonce, cipherTextWithTag, c.headerAD)
	if err != nil {
		return fmt.Errorf("failed to decrypt chunk at offset %d: %w", chunkStartOffset, err)
	}

	c.decryptedBuf = plainChunk

	chunkReader, err := newChunkReader(plainChunk, c.compressionMode)
	if err != nil {
		return fmt.Errorf("failed to decompress chunk at offset %d: %w", chunkStartOffset, err)
	}

	c.chunkReader = chunkReader

	return nil
}

// Validate checks if the provided reader starts with a valid cbcrypto header.
func Validate(r io.Reader) error {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	_, err := parseHeader(header)
	if err != nil {
		return fmt.Errorf("failed to parse header: %w", err)
	}

	return nil
}

// newChunkReader is a helper function that returns a streaming reader for a chunk based on the given compression type.
func newChunkReader(data []byte, compressionType CompressionType) (io.ReadCloser, error) {
	switch compressionType {
	case None:
		return io.NopCloser(bytes.NewReader(data)), nil
	case Snappy:
		decompressed, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, err
		}

		return io.NopCloser(bytes.NewReader(decompressed)), nil
	case ZLib:
		return zlib.NewReader(bytes.NewReader(data))
	case GZip:
		return gzip.NewReader(bytes.NewReader(data))
	case ZStd:
		r, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}

		return r.IOReadCloser(), nil
	case BZip2:
		r, err := bzip2.NewReader(bytes.NewReader(data), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create bzip2 reader: %w", err)
		}

		return io.NopCloser(r), nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm %d", compressionType)
	}
}

// Header represents the parsed metadata from the cbcrypto file header.
type Header struct {
	Version     Version
	Compression CompressionType
	KeyID       string
}

// parseHeader parses the header of a cbcrypto file and returns a Header struct.
func parseHeader(headerData []byte) (*Header, error) {
	if len(headerData) < headerSize {
		return nil, fmt.Errorf("file is too small to be a valid cbcrypto file")
	}

	if !bytes.Equal(headerData[:len(magicBytes)], magicBytes) {
		return nil, fmt.Errorf("does not contain the cbcrypto magic string")
	}

	version := headerData[versionOffset]
	if version > CurrentVersion {
		return nil, fmt.Errorf("unsupported encrypted cbcrypto file version %d", version)
	}

	compression := headerData[compressionOff]

	idLen := int(headerData[idLenOffset])
	if idStartOffset+idLen > saltOffset {
		return nil, fmt.Errorf("key identifier length too large")
	}

	keyID := string(headerData[idStartOffset : idStartOffset+idLen])

	return &Header{
		Version:     version,
		Compression: CompressionType(compression),
		KeyID:       keyID,
	}, nil
}
