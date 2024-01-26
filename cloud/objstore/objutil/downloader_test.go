package objutil

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v4/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v4/objstore/objval"
	fsutil "github.com/couchbase/tools-common/fs/util"
)

func TestMPDownloaderOptionsDefaults(t *testing.T) {
	options := MPDownloaderOptions{}
	options.defaults()
	require.Equal(t, int64(MinPartSize), options.PartSize)
}

func TestNewMPDownloader(t *testing.T) {
	testDir := t.TempDir()

	file, err := os.Create(filepath.Join(testDir, "test.file"))
	require.NoError(t, err)

	defer file.Close()

	options := MPDownloaderOptions{
		Client:    objcli.NewTestClient(t, objval.ProviderAWS),
		Bucket:    "bucket",
		Key:       "key",
		ByteRange: &objval.ByteRange{Start: 64, End: 128},
		Writer:    file,
		Options:   Options{PartSize: 1024},
	}

	downloader := NewMPDownloader(options)

	// Should be updated, the given value is too small
	options.PartSize = MinPartSize

	options.Context = context.Background()

	require.Equal(t, options, downloader.opts)
}

func TestMPDownloaderDownload(t *testing.T) {
	type test struct {
		name     string
		data     []byte
		br       *objval.ByteRange
		expected []byte
	}

	tests := []*test{
		{
			name:     "SmallerThanPartSize",
			data:     []byte("a"),
			expected: []byte("a"),
		},
		{
			name:     "EqualToPartSize",
			data:     []byte(strings.Repeat("a", MinPartSize)),
			expected: []byte(strings.Repeat("a", MinPartSize)),
		},
		{
			name:     "GreaterThanPartSize",
			data:     []byte(strings.Repeat("a", MinPartSize+1)),
			expected: []byte(strings.Repeat("a", MinPartSize+1)),
		},
		{
			name:     "NoSparseFile",
			br:       &objval.ByteRange{Start: 2, End: 4},
			data:     []byte("value"),
			expected: []byte("lue"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				testDir = t.TempDir()
				client  = objcli.NewTestClient(t, objval.ProviderAWS)
			)

			err := client.PutObject(context.Background(), objcli.PutObjectOptions{
				Bucket: "bucket",
				Key:    "key",
				Body:   bytes.NewReader(test.data),
			})
			require.NoError(t, err)

			file, err := fsutil.Create(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			defer file.Close()

			options := DownloadOptions{
				Client:    client,
				Bucket:    "bucket",
				Key:       "key",
				ByteRange: test.br,
				Writer:    file,
			}

			require.NoError(t, Download(options))

			data, err := os.ReadFile(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			require.Equal(t, test.expected, data)
		})
	}
}

func TestMPDownloaderByteRangeUseRemote(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)

	var (
		expected   = &objval.ByteRange{End: 4}
		downloader = NewMPDownloader(MPDownloaderOptions{Client: client, Bucket: "bucket", Key: "key"})
	)

	br, err := downloader.byteRange()
	require.NoError(t, err)
	require.Equal(t, expected, br)
	require.NotSame(t, expected, br)
}

func TestMPDownloaderByteRangeUseProvided(t *testing.T) {
	var (
		expected   = &objval.ByteRange{Start: 64, End: 128}
		downloader = &MPDownloader{opts: MPDownloaderOptions{ByteRange: expected}}
	)

	br, err := downloader.byteRange()
	require.NoError(t, err)
	require.Equal(t, expected, br)
	require.Same(t, expected, br)
}

func TestMPDownloaderInternalDownload(t *testing.T) {
	type test struct {
		name     string
		data     []byte
		br       *objval.ByteRange
		expected []byte
	}

	tests := []*test{
		{
			name:     "SmallerThanPartSize",
			data:     []byte("a"),
			br:       &objval.ByteRange{},
			expected: []byte("a"),
		},
		{
			name:     "EqualToPartSize",
			data:     []byte(strings.Repeat("a", MinPartSize)),
			br:       &objval.ByteRange{End: MinPartSize - 1},
			expected: []byte(strings.Repeat("a", MinPartSize)),
		},
		{
			name:     "GreaterThanPartSize",
			data:     []byte(strings.Repeat("a", MinPartSize+1)),
			br:       &objval.ByteRange{End: MinPartSize},
			expected: []byte(strings.Repeat("a", MinPartSize+1)),
		},
		{
			name: "NonZeroStart",
			data: []byte("value"),
			br:   &objval.ByteRange{Start: 1, End: 3},
			// Not provided a starting byte range so this should be null padded/sparse
			expected: append([]byte{0}, []byte("alu")...),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				testDir = t.TempDir()
				client  = objcli.NewTestClient(t, objval.ProviderAWS)
			)

			err := client.PutObject(context.Background(), objcli.PutObjectOptions{
				Bucket: "bucket",
				Key:    "key",
				Body:   bytes.NewReader(test.data),
			})
			require.NoError(t, err)

			file, err := fsutil.Create(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			defer file.Close()

			options := MPDownloaderOptions{
				Client: client,
				Bucket: "bucket",
				Key:    "key",
				Writer: file,
			}

			downloader := NewMPDownloader(options)

			require.NoError(t, downloader.download(test.br))

			data, err := os.ReadFile(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			require.Equal(t, test.expected, data)
		})
	}
}

func TestMPDownloaderDownloadChunk(t *testing.T) {
	type test struct {
		name     string
		data     []byte
		br       *objval.ByteRange
		expected []byte
	}

	tests := []*test{
		{
			name:     "WholeObject",
			data:     []byte("value"),
			br:       &objval.ByteRange{End: 4},
			expected: []byte("value"),
		},
		{
			name: "NonZeroStart",
			data: []byte("value"),
			br:   &objval.ByteRange{Start: 1, End: 4},
			// Not provided a starting byte range so this should be null padded/sparse
			expected: append([]byte{0}, []byte("alue")...),
		},
		{
			name:     "NonLengthEnd",
			data:     []byte("value"),
			br:       &objval.ByteRange{End: 2},
			expected: []byte("val"),
		},
		{
			name: "BothNonZero",
			data: []byte("value"),
			br:   &objval.ByteRange{Start: 1, End: 2},
			// Not provided a starting byte range so this should be null padded/sparse
			expected: append([]byte{0}, []byte("al")...),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				testDir = t.TempDir()
				client  = objcli.NewTestClient(t, objval.ProviderAWS)
			)

			err := client.PutObject(context.Background(), objcli.PutObjectOptions{
				Bucket: "bucket",
				Key:    "key",
				Body:   bytes.NewReader(test.data),
			})
			require.NoError(t, err)

			file, err := fsutil.Create(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			defer file.Close()

			options := MPDownloaderOptions{
				Client: client,
				Bucket: "bucket",
				Key:    "key",
				Writer: file,
			}

			downloader := NewMPDownloader(options)

			require.NoError(t, downloader.downloadChunk(context.Background(), test.br))

			data, err := os.ReadFile(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			require.Equal(t, test.expected, data)
		})
	}
}
