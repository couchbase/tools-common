package cbcrypto

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

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
func NewCBCWriter(w io.Writer, compression CompressionType, keyID string, key []byte) (*CBCWriter, error) {
	// The header is an 80-byte structure with the following layout:
	//   - Magic         (21 bytes): "\x00Couchbase Encrypted\x00"
	//   - Version       (1 byte)
	//   - Compression   (1 byte)
	//   - Reserved      (4 bytes)
	//   - Key ID Length (1 byte)
	//   - Key ID        (36 bytes)
	//   - Salt          (16 bytes)
	header := make([]byte, headerSize)
	copy(header, magicBytes)
	header[versionOffset] = CurrentVersion
	header[compressionOff] = byte(compression)
	header[idLenOffset] = byte(len(keyID))
	copy(header[idStartOffset:], keyID)

	if _, err := io.ReadFull(rand.Reader, header[saltOffset:]); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	if _, err := w.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	headerAD := make([]byte, headerSize+8)
	copy(headerAD, header)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &CBCWriter{
		writer:   w,
		header:   header,
		headerAD: headerAD,
		offset:   headerSize,
		gcm:      gcm,
	}, nil
}

// AppendChunk compresses, encrypts, and appends a new data chunk.
func (c *CBCWriter) AppendChunk(data io.Reader) error {
	// Compress the data.
	var compressedData bytes.Buffer
	if err := compressData(&compressedData, data, CompressionType(c.header[compressionOff])); err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Update headerAD with the new offset.
	binary.BigEndian.PutUint64(c.headerAD[headerSize:], uint64(c.offset))

	// Encrypt the chunk and write it to the file.
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
