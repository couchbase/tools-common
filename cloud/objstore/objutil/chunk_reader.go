package objutil

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"

	ioiface "github.com/couchbase/tools-common/types/iface"
	"github.com/couchbase/tools-common/utils/maths"
)

// ChunkReader allows data from an 'io.Reader' in chunks of a given size.
type ChunkReader struct {
	size   int64
	reader ioiface.ReadAtSeeker
}

// NewChunkReader creates a new chunk reader which will read chunks of the given size from the provided reader.
func NewChunkReader(reader ioiface.ReadAtSeeker, size int64) ChunkReader {
	return ChunkReader{size: size, reader: reader}
}

// ForEach breaks the 'reader' into chunks running the given function for each chunk created.
func (c ChunkReader) ForEach(fn func(chunk *io.SectionReader) error) error {
	length, err := aws.SeekerLen(c.reader)
	if err != nil {
		return fmt.Errorf("failed to determine length of reader: %w", err)
	}

	err = chunk(length, c.size, func(start, end int64) error {
		return fn(io.NewSectionReader(c.reader, start, maths.Min(end+1, length+1)-start))
	})

	return err
}

// chunk runs the provided function creating 'size' chunks from zero to 'length'.
func chunk(length, size int64, fn func(start, end int64) error) error {
	for start, end := int64(0), size-1; start < length; start, end = start+size, end+size {
		if err := fn(start, maths.Min(end, length-1)); err != nil {
			return err
		}
	}

	return nil
}
