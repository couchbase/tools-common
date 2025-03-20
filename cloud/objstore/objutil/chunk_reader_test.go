package objutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
)

func TestChunkReaderForEachAvoidEmptyChunk(t *testing.T) {
	type test struct {
		name     string
		data     []byte
		chunks   int
		lastSize int64
	}

	tests := []*test{
		{
			name:     "MultipleOfChunkSizeDoNotCreateEmptyChunk",
			data:     []byte("datadata"),
			chunks:   4,
			lastSize: 2,
		},
		{
			name:     "SingleChunkCorrectlyReportSize",
			data:     []byte("d"),
			chunks:   1,
			lastSize: 1,
		},
		{
			name:     "MoreThanChunkSizeCorrectlyReportLastChunkLength",
			data:     []byte("datadatad"),
			chunks:   5,
			lastSize: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var all []*io.SectionReader

			reader := NewChunkReader(bytes.NewReader(test.data), 2)
			require.NoError(t, reader.ForEach(func(chunk *io.SectionReader) error { all = append(all, chunk); return nil }))
			require.Len(t, all, test.chunks)

			length, err := objcli.SeekerLength(all[len(all)-1])
			require.NoError(t, err)
			require.Equal(t, test.lastSize, length)

			buffer := &bytes.Buffer{}

			for _, chunk := range all {
				n, err := io.Copy(buffer, chunk)
				require.NoError(t, err)
				require.NotZero(t, n)
			}

			require.Equal(t, test.data, buffer.Bytes())
		})
	}
}
