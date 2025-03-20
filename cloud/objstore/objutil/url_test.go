package objutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
	errutil "github.com/couchbase/tools-common/errors/util"
)

func TestParseCloudOrFileURL(t *testing.T) {
	type test struct {
		name             string
		input            string
		expectedBucket   string
		expectedPath     string
		expectedProvider objval.Provider
		expectedError    bool
	}

	cwd, err := os.Getwd()
	require.NoError(t, err)

	tests := []test{
		{
			name:         "absolute-local-path",
			input:        "/archive",
			expectedPath: "/archive",
		},
		{
			name:         "non-absolute-local-path",
			input:        "archive",
			expectedPath: filepath.Join(cwd, "archive"),
		},
		{
			name:         "absolute-local-path-with-file-prefix",
			input:        "file:///archive",
			expectedPath: "/archive",
		},
		{
			name:         "non-absolute-local-path-with-file-prefix",
			input:        "file://archive",
			expectedPath: filepath.Join(cwd, "archive"),
		},
		{
			name:             "s3-bucket-and-archive-path",
			input:            "s3://bucket/archive",
			expectedBucket:   "bucket",
			expectedPath:     "archive",
			expectedProvider: objval.ProviderAWS,
		},
		{
			name:             "s3-bucket-no-archive-path",
			input:            "s3://bucket",
			expectedBucket:   "bucket",
			expectedProvider: objval.ProviderAWS,
		},
		{
			name:             "s3-bucket-should-be-cleaned-archive-path",
			input:            "s3://bucket//archive",
			expectedBucket:   "bucket",
			expectedPath:     "archive",
			expectedProvider: objval.ProviderAWS,
		},
		{
			name:          "s3-no-bucket",
			input:         "s3://",
			expectedError: true,
		},
		{
			name:             "az-container-and-archive-path",
			input:            "az://container/archive",
			expectedBucket:   "container",
			expectedPath:     "archive",
			expectedProvider: objval.ProviderAzure,
		},
		{
			name:             "az-container-no-archive-path",
			input:            "az://container",
			expectedBucket:   "container",
			expectedProvider: objval.ProviderAzure,
		},
		{
			name:             "az-container-should-be-cleaned-archive-path",
			input:            "az://container//archive",
			expectedBucket:   "container",
			expectedPath:     "archive",
			expectedProvider: objval.ProviderAzure,
		},
		{
			name:          "az-no-container",
			input:         "az://",
			expectedError: true,
		},
		{
			name:             "gs-bucket-and-archive-path",
			input:            "gs://bucket/archive",
			expectedBucket:   "bucket",
			expectedPath:     "archive",
			expectedProvider: objval.ProviderGCP,
		},
		{
			name:             "gs-bucket-no-archive-path",
			input:            "gs://bucket",
			expectedBucket:   "bucket",
			expectedProvider: objval.ProviderGCP,
		},
		{
			name:             "gs-bucket-should-be-cleaned-archive-path",
			input:            "gs://bucket//archive",
			expectedBucket:   "bucket",
			expectedPath:     "archive",
			expectedProvider: objval.ProviderGCP,
		},
		{
			name:          "gs-no-bucket",
			input:         "gs://",
			expectedError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			url, err := ParseCloudOrFileURL(test.input)
			if test.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedBucket, url.Bucket)
			require.Equal(t, test.expectedPath, url.Path)
			require.Equal(t, test.expectedProvider, url.Provider)
		})
	}
}

func TestParseCloudOrFileURLArgumentSuggestionsInError(t *testing.T) {
	_, err := ParseCloudOrFileURL("unknown://path/to/archive")
	require.Error(t, err)
	require.True(t, errutil.Contains(err, "[file://, s3://, az://, gs://]"))
}
