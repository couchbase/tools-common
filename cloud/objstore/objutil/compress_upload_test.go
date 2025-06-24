package objutil

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"log/slog"
	"math"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	"github.com/couchbase/tools-common/functional/slices"
	"github.com/couchbase/tools-common/testing/mock/matchers"
	"github.com/couchbase/tools-common/types/v2/ptr"
)

const partSize = 1024

// paths is a list of objects to create. This ensures we test a few different cases: objects the size of partSize, an
// odd size (i.e. not devisable or a factor of partSize), a multiple of partSize and +/- 1.
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

		err = cli.PutObject(context.Background(), objcli.PutObjectOptions{
			Bucket: "bucket",
			Key:    path.path,
			Body:   bytes.NewReader(buf[0:path.size]),
		})
		require.NoError(t, err)
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
		SourceClient:      objcli.NewTestClient(t, objval.ProviderAWS),
		DestinationClient: objcli.NewTestClient(t, objval.ProviderGCP),
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
		SourceClient:      opts.SourceClient,
		DestinationClient: opts.DestinationClient,
		Logger:            slog.Default(),
	}

	require.Equal(t, expected, opts)
}

func TestCompressUploadOptionsMissing(t *testing.T) {
	_, err := CompressObjects(CompressObjectsOptions{})
	require.Error(t, err)
}

// TestCompressUploadIterateError tests to see that CompressObjects doesn't hang
// when iterate returns an error (MB-55967)
func TestCompressUploadIterateError(t *testing.T) {
	cli := objcli.NewTestClient(t, objval.ProviderAWS)

	_, err := CompressObjects(CompressObjectsOptions{
		SourceClient:      cli,
		DestinationClient: cli,
		SourceBucket:      "bucket",
		Prefix:            "prefix",
		DestinationBucket: "bucket",
		Destination:       "export.zip",
		// Include and Exclude both being non-nil means iterate will return an error
		Include:  []*regexp.Regexp{{}},
		Exclude:  []*regexp.Regexp{{}},
		Checksum: md5.New(),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "include/exclude are mutually exclusive")
}

func TestCompressUpload(t *testing.T) {
	cli := setupTestClient(t)

	_, err := CompressObjects(CompressObjectsOptions{
		Options:           Options{PartSize: partSize},
		SourceClient:      cli,
		DestinationClient: cli,
		SourceBucket:      "bucket",
		Prefix:            "prefix",
		DestinationBucket: "bucket",
		Destination:       "export.zip",
		Checksum:          sha256.New(),
	})

	require.NoError(t, err)

	require.Contains(t, cli.Buckets["bucket"], objval.TestObjectIdentifier{Key: "export.zip"})

	data := cli.Buckets["bucket"][objval.TestObjectIdentifier{Key: "export.zip"}].Body

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
		require.Equal(t, cli.Buckets["bucket"][objval.TestObjectIdentifier{Key: path.path}].Body, buf)
	}
}

func TestCompressUploadNoPrefix(t *testing.T) {
	cli := setupTestClient(t)

	_, err := CompressObjects(CompressObjectsOptions{
		Options:           Options{PartSize: partSize},
		SourceClient:      cli,
		DestinationClient: cli,
		SourceBucket:      "bucket",
		DestinationBucket: "bucket",
		Destination:       "export.zip",
		Checksum:          sha256.New(),
	})

	require.NoError(t, err)

	require.Contains(t, cli.Buckets["bucket"], objval.TestObjectIdentifier{Key: "export.zip"})

	data := cli.Buckets["bucket"][objval.TestObjectIdentifier{Key: "export.zip"}].Body

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
		require.Equal(t, cli.Buckets["bucket"][objval.TestObjectIdentifier{Key: path.path}].Body, buf)
	}
}

func TestCompressUploadProgressReporting(t *testing.T) {
	var (
		cli = setupTestClient(t)

		reports = make([]float64, 0, len(paths))
		fn      = func(progress float64) { reports = append(reports, progress) }
	)

	_, err := CompressObjects(CompressObjectsOptions{
		Options:                Options{PartSize: partSize},
		SourceClient:           cli,
		DestinationClient:      cli,
		SourceBucket:           "bucket",
		Prefix:                 "prefix",
		DestinationBucket:      "bucket",
		Destination:            "export.zip",
		ProgressReportCallback: fn,
		Checksum:               sha256.New(),
	})

	require.NoError(t, err)

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

		objectSizes = slices.Filter(objectSizes, func(e int64) bool { return e != size })
	}
}

// TestCompressUploadIterateError tests to see that CompressObjects doesn't hang if the upload goroutine exits with an
// error (MB-57297).
func TestCompressUploadUploadError(t *testing.T) {
	const size = 10 * 1024 * 1024

	cli := objcli.MockClient{}
	cli.
		On("IterateObjects", matchers.Context, mock.Anything).
		Return(func(_ context.Context, opts objcli.IterateObjectsOptions) error {
			_ = opts.Func(&objval.ObjectAttrs{
				Key:  "foo",
				Size: ptr.To[int64](size),
			})

			return nil
		})

	cli.
		On("GetObject", matchers.Context, mock.Anything).
		Return(func(_ context.Context, _ objcli.GetObjectOptions) (*objval.Object, error) {
			var (
				body = make([]byte, size)
				r    = io.NopCloser(bytes.NewReader(body))
			)

			s, err := rand.Read(body)
			require.NoError(t, err)
			require.Equal(t, size, s)

			return &objval.Object{Body: r}, nil
		})

	cli.On("CreateMultipartUpload", matchers.Context, mock.Anything).Return("mp-001", nil)

	// To make the scenario more realistic we allow a couple of parts to get uploaded before returning an error.
	var i int

	cli.
		On("UploadPart", matchers.Context, mock.Anything).
		Return(func(_ context.Context, _ objcli.UploadPartOptions) (objval.Part, error) {
			if i < 2 {
				return objval.Part{}, nil
			}

			i++

			return objval.Part{}, assert.AnError
		})

	cli.
		On("AbortMultipartUpload", matchers.Context, mock.Anything).
		Return(nil)

	_, err := CompressObjects(CompressObjectsOptions{
		Options:           Options{PartSize: 1024},
		SourceClient:      &cli,
		DestinationClient: &cli,
		SourceBucket:      "bucket",
		Prefix:            "prefix",
		DestinationBucket: "bucket",
		Destination:       "export.zip",
		Checksum:          sha256.New(),
	})
	require.ErrorIs(t, err, ErrMPUploaderExceededMaxPartCount)

	cli.AssertExpectations(t)
}

func TestUploadFromReader(t *testing.T) {
	client := setupTestClient(t)

	options := CompressObjectsOptions{
		Options:           Options{PartSize: partSize},
		SourceClient:      client,
		DestinationClient: client,
		Prefix:            "avocados.png",
		SourceBucket:      "bucket-source",
		DestinationBucket: "bucket-destination",
		Destination:       "avocados.zip",
		Checksum:          sha256.New(),
	}

	checksum, err := CompressObjects(options)
	require.NoError(t, err)

	data := client.Buckets["bucket-destination"][objval.TestObjectIdentifier{Key: "avocados.zip"}].Body
	require.NotNil(t, data)

	hasher := sha256.New()
	_, err = hasher.Write(data)
	require.NoError(t, err)

	require.Equal(t, checksum, hasher.Sum(nil), "checksums mismatch")
}

func TestUploadToAnotherClient(t *testing.T) {
	var (
		src = setupTestClient(t)
		dst = setupTestClient(t)
	)

	options := CompressObjectsOptions{
		Options:           Options{PartSize: partSize},
		SourceClient:      src,
		DestinationClient: dst,
		Prefix:            "avocados.png",
		SourceBucket:      "bucket-source",
		DestinationBucket: "bucket-destination",
		Destination:       "avocados.zip",
		Checksum:          sha256.New(),
	}

	checksum, err := CompressObjects(options)
	require.NoError(t, err)

	data := dst.Buckets["bucket-destination"][objval.TestObjectIdentifier{Key: "avocados.zip"}].Body
	require.NotNil(t, data)
	require.NotContains(t, src.Buckets["bucket-source"], objval.TestObjectIdentifier{Key: "avocados.zip"})

	hasher := sha256.New()
	_, err = hasher.Write(data)
	require.NoError(t, err)

	require.Equal(t, checksum, hasher.Sum(nil), "checksums mismatch")
}
