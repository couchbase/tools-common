package objutil

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/couchbase/tools-common/maths"
)

// ChunkReader allows data from an 'io.Reader' in chunks of a given size.
type ChunkReader struct {
	size   int64
	reader ReadAtSeeker
}

// NewChunkReader creates a new chunk reader which will read chunks of the given size from the provided reader.
func NewChunkReader(reader ReadAtSeeker, chunkSize int64) ChunkReader {
	return ChunkReader{size: chunkSize, reader: reader}
}

// ForEach breaks the 'reader' into chunks running the given function for each chunk created.
func (c ChunkReader) ForEach(fn func(chunk *io.SectionReader) error) error {
	length, err := aws.SeekerLen(c.reader)
	if err != nil {
		return fmt.Errorf("failed to determine length of reader: %w", err)
	}

	for s, e := int64(0), c.size-1; s < length; s, e = s+c.size, e+c.size {
		if err := fn(io.NewSectionReader(c.reader, s, maths.Min(e+1, length)-s)); err != nil {
			return err
		}
	}

	return nil
}
