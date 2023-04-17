package objutil

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"math"
	"math/rand"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/objstore/objval"
	"github.com/couchbase/tools-common/slice"
)

const partSize = 1024

// paths is a list of objects to create. This ensures we test a few different cases: objects the size of partSize, an
// odd size (i.e. not divisable or a factor of partSize), a multiple of partSize and +/- 1.
var paths = []struct {
	path string
	size int64
}{
	{path: "prefix/foo.txt", size: partSize},
	{path: "prefix/1/bar.txt", size: 147},
	{path: "prefix/1/2/baz.txt", size: 4 * partSize},
	{path: "prefix/1/2/boo.txt", size: partSize + 1},
	{path: "prefix/1/2/moo.txt", size: partSize - 1},
}

// setupTestClient populates a test objcli.Client with the objects in paths and returns it.
func setupTestClient(t *testing.T) *objcli.TestClient {
	var (
		testData = make([]byte, 4*partSize)
		n, err   = rand.Read(testData)
	)

	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	cli := objcli.NewTestClient(t, objval.ProviderAWS)

	for _, path := range paths {
		var (
			buf    = make([]byte, path.size)
			n, err = rand.Read(buf)
		)

		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.NoError(t, cli.PutObject(context.Background(), "bucket", path.path, bytes.NewReader(buf[0:path.size])))
	}

	return cli
}

func TestStripPrefix(t *testing.T) {
	tests := []struct{ name, prefix, fullPath, expected string }{
		{
			name:     "NoTrailingSlash",
			prefix:   "foo/bar",
			fullPath: "foo/bar/baz/01.txt",
			expected: "bar/baz/01.txt",
		},
		{
			name:     "TrailingSlash",
			prefix:   "foo/bar/",
			fullPath: "foo/bar/baz/01.txt",
			expected: "baz/01.txt",
		},
		{
			name:     "DoesNothingIfNotAPrefix",
			prefix:   "foo/",
			fullPath: "bar/baz/01.txt",
			expected: "bar/baz/01.txt",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, stripPrefix(test.prefix, test.fullPath))
		})
	}
}

func TestCompressUploadOptionsDefaults(t *testing.T) {
	opts := CompressObjectsOptions{
		Client:            objcli.NewTestClient(t, objval.ProviderAWS),
		SourceBucket:      "bucket",
		DestinationBucket: "bucket",
		Prefix:            "prefix",
		Destination:       "dest",
	}

	opts.defaults()

	expected := CompressObjectsOptions{
		Options: Options{
			Context:  opts.Context,
			PartSize: objaws.MinUploadSize,
		},
		SourceBucket:      "bucket",
		DestinationBucket: "bucket",
		Prefix:            "prefix",
		Destination:       "dest",
		PartUploadWorkers: 4,
		Client:            opts.Client,
	}

	require.Equal(t, expected, opts)
}

func TestCompressUploadOptionsMissing(t *testing.T) {
	require.Error(t, CompressObjects(CompressObjectsOptions{}))
}

// TestCompressUploadIterateError tests to see that CompressObjects doesn't hang
// when iterate returns an error (MB-55967)
func TestCompressUploadIterateError(t *testing.T) {
	cli := objcli.NewTestClient(t, objval.ProviderAWS)

	require.Error(t, CompressObjects(CompressObjectsOptions{
		Client:            cli,
		SourceBucket:      "bucket",
		Prefix:            "prefix",
		DestinationBucket: "bucket",
		Destination:       "export.zip",
		// Include and Exclude both being non-nil means iterate will return an error
		Include: []*regexp.Regexp{{}},
		Exclude: []*regexp.Regexp{{}},
	}))
}

func TestCompressUpload(t *testing.T) {
	cli := setupTestClient(t)
	require.NoError(t, CompressObjects(CompressObjectsOptions{
		Options:           Options{PartSize: partSize},
		Client:            cli,
		SourceBucket:      "bucket",
		Prefix:            "prefix",
		DestinationBucket: "bucket",
		Destination:       "export.zip",
	}))

	require.Contains(t, cli.Buckets["bucket"], "export.zip")

	data := cli.Buckets["bucket"]["export.zip"].Body

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	for _, path := range paths {
		file, err := zr.Open(path.path)
		require.NoError(t, err)

		defer file.Close()

		stat, err := file.Stat()
		require.NoError(t, err)
		require.Equal(t, path.size, stat.Size())

		buf, err := io.ReadAll(file)
		require.NoError(t, err)
		require.Equal(t, cli.Buckets["bucket"][path.path].Body, buf)
	}
}

func TestCompressUploadProgressReporting(t *testing.T) {
	var (
		cli = setupTestClient(t)

		reports = make([]float64, 0, len(paths))
		fn      = func(progress float64) { reports = append(reports, progress) }
	)

	require.NoError(t, CompressObjects(CompressObjectsOptions{
		Options:                Options{PartSize: partSize},
		Client:                 cli,
		SourceBucket:           "bucket",
		Prefix:                 "prefix",
		DestinationBucket:      "bucket",
		Destination:            "export.zip",
		ProgressReportCallback: fn,
	}))

	var total float64
	for _, path := range paths {
		total += float64(path.size)
	}

	// The iteration of the objects is not defined, so we can get different progress reports every time this test is
	// run. To work around this we ensure the difference between each progress report is equal to one of the sizes of
	// the files.
	objectSizes := make([]int64, len(paths))
	for i := range paths {
		objectSizes[i] = paths[i].size
	}

	for i, report := range reports {
		require.Greater(t, report, 0.0)
		require.LessOrEqual(t, report, 1.0)

		diff := report
		if i > 0 {
			diff = report - reports[i-1]
		}

		// We need to round here to avoid precision errors.
		size := int64(math.Round(diff * total))
		require.Contains(t, objectSizes, size)

		objectSizes = slice.Filter(objectSizes, func(e int64) bool { return e != size })
	}
}
