package objutil

import (
	"strings"
	"testing"
	"time"

	"github.com/couchbase/tools-common/cloud/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/objstore/objval"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMPUploadOptionsDefaults(t *testing.T) {
	options := MPUploaderOptions{}
	require.Nil(t, options.OnPartComplete)

	options.defaults()
	require.NotNil(t, options.OnPartComplete)
	require.NoError(t, options.OnPartComplete(nil, objval.Part{}))
}

func TestNewMPUploader(t *testing.T) {
	options := MPUploaderOptions{
		Client: objcli.NewTestClient(t, objval.ProviderAWS),
		Bucket: "bucket",
		Key:    "key",
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NotNil(t, uploader.opts.OnPartComplete)
	require.NoError(t, uploader.opts.OnPartComplete(nil, objval.Part{}))

	require.NotEmpty(t, uploader.opts.ID)
	require.Equal(t, uploader.opts.ID, uploader.UploadID())

	require.NotNil(t, uploader.pool)
}

func TestNewMPUploaderExistingUpload(t *testing.T) {
	options := MPUploaderOptions{
		Client: objcli.NewTestClient(t, objval.ProviderAWS),
		Bucket: "bucket",
		ID:     "id",
		Key:    "key",
		Parts:  []objval.Part{{ID: "id2", Number: 2}, {ID: "id42", Number: 42}, {ID: "id1", Number: 1}},
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NotNil(t, uploader.opts.OnPartComplete)
	require.NoError(t, uploader.opts.OnPartComplete(nil, objval.Part{}))

	require.Equal(t, "id", uploader.opts.ID)
	require.Equal(t, 42, uploader.number)

	require.NotNil(t, uploader.pool)
}

func TestMPUploaderUpload(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := MPUploaderOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("body")))
	require.NoError(t, uploader.Commit())

	require.Len(t, uploader.opts.Parts, 1)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)
	require.Contains(t, client.Buckets["bucket"], "key")
	require.Equal(t, []byte("body"), client.Buckets["bucket"]["key"].Body)
}

func TestMPUploaderUploadWithMetaAndOnPartComplete(t *testing.T) {
	var (
		client   = objcli.NewTestClient(t, objval.ProviderAWS)
		metadata any
		part     objval.Part
	)

	options := MPUploaderOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		OnPartComplete: func(m any, p objval.Part) error {
			metadata = m
			part = p

			return nil
		},
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.UploadWithMeta(42, strings.NewReader("body")))
	require.NoError(t, uploader.Commit())

	require.Len(t, uploader.opts.Parts, 1)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)
	require.Contains(t, client.Buckets["bucket"], "key")
	require.Equal(t, []byte("body"), client.Buckets["bucket"]["key"].Body)

	require.Equal(t, 42, metadata)
	require.NotZero(t, part)
}

func TestMPUploaderUploadWithOnPartCompletePropagateUserError(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := MPUploaderOptions{
		Client:         client,
		Bucket:         "bucket",
		Key:            "key",
		OnPartComplete: func(_ any, _ objval.Part) error { return assert.AnError },
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("body")))
	require.ErrorIs(t, uploader.Commit(), assert.AnError)
}

func TestMPUploaderUploadAlmostGreaterThanMaxCount(t *testing.T) {
	options := MPUploaderOptions{
		Client: objcli.NewTestClient(t, objval.ProviderAWS),
		Bucket: "bucket",
		ID:     "id",
		Key:    "key",
		Parts:  make([]objval.Part, MaxUploadParts-1),
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("body")))
}

func TestMPUploaderUploadGreaterThanMaxCount(t *testing.T) {
	options := MPUploaderOptions{
		Client: objcli.NewTestClient(t, objval.ProviderAWS),
		Bucket: "bucket",
		ID:     "id",
		Key:    "key",
		Parts:  make([]objval.Part, MaxUploadParts),
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.ErrorIs(t, uploader.Upload(strings.NewReader("body")), ErrMPUploaderExceededMaxPartCount)
}

func TestMPUploaderUploadGreaterThanMaxCountButOnlyOnePart(t *testing.T) {
	options := MPUploaderOptions{
		Client: objcli.NewTestClient(t, objval.ProviderAWS),
		Bucket: "bucket",
		ID:     "id",
		Key:    "key",
		Parts:  []objval.Part{{Number: MaxUploadParts}},
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("body")))
}

func TestMPUploaderStop(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := MPUploaderOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("1")))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, uploader.opts.Parts, 1)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)

	require.NoError(t, uploader.Upload(strings.NewReader("2")))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, uploader.opts.Parts, 2)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)

	require.NoError(t, uploader.Stop())
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2) // Should not be committed or aborted

	require.ErrorIs(t, uploader.Stop(), ErrMPUploaderAlreadyStopped)
}

func TestMPUploaderAbort(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := MPUploaderOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("body")))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, uploader.opts.Parts, 1)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)

	require.NoError(t, uploader.Abort())
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 0)
}

func TestMPUploaderCommit(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := MPUploaderOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
	}

	uploader, err := NewMPUploader(options)
	require.NoError(t, err)

	defer uploader.Abort() //nolint:errcheck

	require.NoError(t, uploader.Upload(strings.NewReader("1")))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, uploader.opts.Parts, 1)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)

	require.NoError(t, uploader.Upload(strings.NewReader("2")))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, uploader.opts.Parts, 2)
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)

	require.NoError(t, uploader.Commit())
	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 1)
	require.Contains(t, client.Buckets["bucket"], "key")
	require.Equal(t, []byte("12"), client.Buckets["bucket"]["key"].Body)
}
