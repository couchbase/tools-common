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

	"github.com/couchbase/tools-common/cloud/v4/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v4/objstore/objval"
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
		name string
		data []byte
	}

	tests := []*test{
		{
			name: "SmallerThanPartSize",
			data: []byte("a"),
		},
		{
			name: "EqualToPartSize",
			data: []byte(strings.Repeat("a", MinPartSize)),
		},
		{
			name: "GreaterThanPartSize",
			data: []byte(strings.Repeat("a", MinPartSize+1)),
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
				Client: client,
				Bucket: "bucket",
				Key:    "key",
				Writer: file,
			}

			require.NoError(t, Download(options))

			data, err := os.ReadFile(filepath.Join(testDir, "test.file"))
			require.NoError(t, err)
			require.Equal(t, test.data, data)
		})
	}
}

func TestDownloadTrackExpectedWrites(t *testing.T) {
	var (
		client = objcli.NewTestClient(t, objval.ProviderAWS)
		writer = &tracker{}
	)

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader(strings.Repeat("a", MinPartSize*2+42)),
	})
	require.NoError(t, err)

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

	err := client.PutObject(context.Background(), objcli.PutObjectOptions{
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("value"),
	})
	require.NoError(t, err)

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
