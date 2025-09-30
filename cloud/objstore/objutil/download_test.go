package objutil

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
	fsutil "github.com/couchbase/tools-common/fs/util"

	"github.com/stretchr/testify/require"
)

type tracked struct {
	offset int64
	length int64
}

type tracker struct {
	lock   sync.Mutex
	writes []tracked
}

func (o *tracker) WriteAt(data []byte, offset int64) (int, error) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.writes = append(o.writes, tracked{offset: offset, length: int64(len(data))})

	return len(data), nil
}

func TestDownload(t *testing.T) {
	type test struct {
		name  string
		data1 []byte
		data2 []byte
	}

	tests := []*test{
		{
			name:  "SmallerThanPartSize",
			data1: []byte("b"),
			data2: []byte("a"),
		},
		{
			name:  "EqualToPartSize",
			data1: []byte(strings.Repeat("b", MinPartSize)),
			data2: []byte(strings.Repeat("a", MinPartSize)),
		},
		{
			name:  "GreaterThanPartSize",
			data1: []byte(strings.Repeat("b", MinPartSize+1)),
			data2: []byte(strings.Repeat("a", MinPartSize+1)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				testDir = t.TempDir()
				client  = objcli.NewTestClient(t, objval.ProviderAWS)
			)

			attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
				Bucket: "bucket",
				Key:    "key",
				Body:   bytes.NewReader(test.data1),
			})
			require.NoError(t, err)

			require.Equal(t, "key", attrs.Key)
			require.Equal(t, int64(len(test.data1)), *attrs.Size)
			require.NotEmpty(t, attrs.ETag)
			require.True(t, time.Now().After(*attrs.LastModified))

			attrs, err = client.PutObject(context.Background(), objcli.PutObjectOptions{
				Bucket: "bucket",
				Key:    "key",
				Body:   bytes.NewReader(test.data2),
			})
			require.NoError(t, err)

			require.Equal(t, "key", attrs.Key)
			require.Equal(t, int64(len(test.data2)), *attrs.Size)
			require.NotEmpty(t, attrs.ETag)
			require.True(t, time.Now().After(*attrs.LastModified))

			file, err := fsutil.Create(filepath.Join(testDir, "test.file2"))
			require.NoError(t, err)
			defer file.Close()

			options := DownloadOptions{
				Client: client,
				Bucket: "bucket",
				Key:    "key",
				Writer: file,
			}

			require.NoError(t, Download(options))

			data, err := os.ReadFile(filepath.Join(testDir, "test.file2"))
			require.NoError(t, err)
			require.Equal(t, test.data2, data)

			file, err = fsutil.Create(filepath.Join(testDir, "test.file1"))
			require.NoError(t, err)
			defer file.Close()

			firstVersionID := ""

			for _, obj := range client.Buckets["bucket"] {
				if !obj.IsCurrentVersion {
					firstVersionID = obj.VersionID
					break
				}
			}

			options = DownloadOptions{
				Client:    client,
				Bucket:    "bucket",
				Key:       "key",
				VersionID: firstVersionID,
				Writer:    file,
			}

			require.NoError(t, Download(options))

			data, err = os.ReadFile(filepath.Join(testDir, "test.file1"))
			require.NoError(t, err)
			require.Equal(t, test.data1, data)
		})
	}
}

func TestDownloadTrackExpectedWrites(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		writer = &tracker{}
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader(strings.Repeat("a", MinPartSize*2+42)),
	})
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)
	require.Equal(t, int64(MinPartSize*2+42), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	options := DownloadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Writer: writer,
	}

	require.NoError(t, Download(options))

	// Writes may happen in any arbitrary order, sort by offset
	sort.Slice(writer.writes, func(i, j int) bool { return writer.writes[i].offset < writer.writes[j].offset })

	expected := []tracked{
		{length: MinPartSize},
		{offset: MinPartSize, length: MinPartSize},
		{offset: MinPartSize * 2, length: 42},
	}

	require.Equal(t, expected, writer.writes)
}

func TestDownloadWithByteRange(t *testing.T) {
	var (
		testDir = t.TempDir()
		client  = objcli.NewTestClient(t, objval.ProviderAWS)
	)

	attrs, err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)
	require.Equal(t, int64(5), *attrs.Size)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	file, err := fsutil.Create(filepath.Join(testDir, "test.file"))
	require.NoError(t, err)

	defer file.Close()

	options := DownloadOptions{
		Client:    client,
		Bucket:    "bucket",
		Key:       "key",
		ByteRange: &objval.ByteRange{Start: 1, End: 3},
		Writer:    file,
	}

	require.NoError(t, Download(options))

	data, err := os.ReadFile(filepath.Join(testDir, "test.file"))
	require.NoError(t, err)
	require.Equal(t, []byte("alu"), data)
}
