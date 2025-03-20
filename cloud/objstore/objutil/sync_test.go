package objutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	fsutil "github.com/couchbase/tools-common/fs/util"
)

type fileDesc struct{ path, contents string }

const (
	fileBytes = 4
	// We want to read one file every 50ms
	fileInterval = time.Millisecond * 50
	interval     = fileInterval / fileBytes
	leeway       = fileInterval / 10
)

var files = []fileDesc{
	{path: "test.txt", contents: "1234"},
	{path: "1/test.txt", contents: "4567"},
	{path: "1/2/3/test.txt", contents: "7890"},
}

func TestUploadDirectory(t *testing.T) {
	tmp := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmp, "bar", "1", "2", "3"), 0o777))

	for _, file := range files {
		require.NoError(t, fsutil.WriteFile(filepath.Join(tmp, "bar", file.path), []byte(file.contents), 0o666))
	}

	var (
		src         = strings.TrimSuffix(filepath.Join(tmp, "bar"), string(os.PathSeparator))
		srcTrailing = src + string(os.PathSeparator)
		dst         = "s3://bucket/foo"
		dstTrailing = dst + "/"
	)

	tests := []struct{ src, dst, subdir string }{
		{src: src, dst: dst, subdir: "bar"},
		{src: srcTrailing, dst: dst},
		{src: srcTrailing, dst: dstTrailing},
		{src: src, dst: dstTrailing, subdir: "bar"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s-%s", test.src, test.dst), func(t *testing.T) {
			client := objcli.NewTestClient(t, objval.ProviderAWS)

			require.NoError(t, Sync(SyncOptions{
				Client:      client,
				Source:      test.src,
				Destination: test.dst,
			}))

			require.Len(t, client.Buckets, 1)
			require.Len(t, client.Buckets["bucket"], len(files))

			for _, file := range files {
				key := filepath.Join("foo", test.subdir, file.path)
				require.Contains(t, client.Buckets["bucket"], key)
				require.Equal(t, []byte(file.contents), client.Buckets["bucket"][key].Body)
			}
		})
	}
}

func TestDownloadDirectory(t *testing.T) {
	tmp := t.TempDir()

	client := objcli.NewTestClient(t, objval.ProviderAWS)

	for _, file := range files {
		objcli.TestUploadRAW(t, client, filepath.Join("foo", "bar", file.path), []byte(file.contents))
	}

	var (
		src         = "s3://bucket/foo/bar"
		srcTrailing = src + "/"
		dst         = strings.TrimSuffix(tmp, string(os.PathSeparator))
		dstTrailing = dst + string(os.PathSeparator)
	)

	tests := []struct{ src, dst, subdir string }{
		{src: src, dst: dst, subdir: "bar"},
		{src: srcTrailing, dst: dst},
		{src: srcTrailing, dst: dstTrailing},
		{src: src, dst: dstTrailing, subdir: "bar"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s-%s", test.src, test.dst), func(t *testing.T) {
			require.NoError(t, Sync(SyncOptions{
				Client:      client,
				Source:      test.src,
				Destination: test.dst,
			}))

			for _, file := range files {
				path := filepath.Join(tmp, test.subdir, file.path)
				contents, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, []byte(file.contents), contents)
			}
		})
	}
}

func TestUploadFile(t *testing.T) {
	var (
		tmp = t.TempDir()
		src = filepath.Join(tmp, "test.txt")
	)

	err := fsutil.WriteFile(src, []byte("0123456789"), 0o666)
	require.NoError(t, err)

	client := objcli.NewTestClient(t, objval.ProviderAWS)

	// Sync only works on directories, so this should error
	require.Error(t, Sync(SyncOptions{
		Client:      client,
		Source:      src,
		Destination: "s3://bucket/test.txt",
	}))
}

func TestDownloadRateLimited(t *testing.T) {
	var (
		tmp     = t.TempDir()
		client  = objcli.NewTestClient(t, objval.ProviderAWS)
		limiter = rate.NewLimiter(rate.Every(interval), fileBytes)
	)

	for _, file := range files {
		objcli.TestUploadRAW(t, client, filepath.Join("foo", "bar", file.path), []byte(file.contents))
	}

	start := time.Now()

	require.NoError(t, Sync(SyncOptions{
		Limiter:     limiter,
		Client:      client,
		Source:      "s3://bucket/foo/bar",
		Destination: tmp,
	}))

	require.Greater(t, time.Now(), start.Add(time.Duration(len(files)-1)*fileInterval-leeway))
}

func TestUploadRateLimited(t *testing.T) {
	var (
		tmp     = t.TempDir()
		client  = objcli.NewTestClient(t, objval.ProviderAWS)
		limiter = rate.NewLimiter(rate.Every(interval), fileBytes)
	)

	require.NoError(t, os.MkdirAll(filepath.Join(tmp, "bar", "1", "2", "3"), 0o777))

	for _, file := range files {
		require.NoError(t, fsutil.WriteFile(filepath.Join(tmp, "bar", file.path), []byte(file.contents), 0o666))
	}

	start := time.Now()

	require.NoError(t, Sync(SyncOptions{
		Limiter:     limiter,
		Client:      client,
		Source:      tmp,
		Destination: "s3://bucket/foo/bar",
	}))

	require.Greater(t, time.Now(), start.Add(time.Duration(len(files)-1)*fileInterval-leeway))
}
