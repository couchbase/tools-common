package objutil

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"

	"github.com/stretchr/testify/require"
)

func TestUploadOptionsDefaults(t *testing.T) {
	options := UploadOptions{}
	options.defaults()
	require.Equal(t, int64(MinPartSize), options.PartSize)
	require.Equal(t, int64(MPUThreshold), options.MPUThreshold)
}

func TestUploadObjectLessThanThreshold(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("body"),
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, []byte("body"), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)
	require.Equal(t, objval.LockTypeUndefined, client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockType)
}

func TestUploadObjectLessThanThresholdLock(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	now := time.Now()

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("body"),
		Lock:   objcli.NewComplianceLock(now.AddDate(0, 0, 5)),
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, []byte("body"), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)
	require.Equal(
		t,
		objval.LockTypeCompliance,
		client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockType,
	)
	require.Equal(
		t,
		options.Lock.Expiration,
		*client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockExpiration,
	)
}

func TestUploadObjectLessThanThresholdTwoTimes(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   strings.NewReader("body"),
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, []byte("body"), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)
	require.Equal(t, objval.LockTypeUndefined, client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockType)

	attrs, err = Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))
}

func TestUploadObjectLessThanThresholdIfAbsent(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client:       client,
		Bucket:       "bucket",
		Key:          "key",
		Body:         strings.NewReader("body"),
		Precondition: objcli.OperationPreconditionOnlyIfAbsent,
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, []byte("body"), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)
	require.Equal(t, objval.LockTypeUndefined, client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockType)

	_, err = Upload(options)
	require.Error(t, err)
}

func TestUploadObjectGreaterThanThreshold(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   bytes.NewReader(make([]byte, MPUThreshold+1)),
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)
	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, make([]byte, MPUThreshold+1), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)
	require.Equal(t, objval.LockTypeUndefined, client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockType)
}

func TestUploadObjectGreaterThanThresholdLock(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	now := time.Now()

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   bytes.NewReader(make([]byte, MPUThreshold+1)),
		Lock:   objcli.NewComplianceLock(now.AddDate(0, 0, 3)),
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 10)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(
		t,
		make([]byte, MPUThreshold+1),
		client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body,
	)
	require.Equal(
		t,
		objval.LockTypeCompliance,
		client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockType,
	)
	require.Equal(
		t,
		now.AddDate(0, 0, 3),
		*client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].LockExpiration,
	)
}

func TestUploadObjectGreaterThanThresholdTwoTimes(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client: client,
		Bucket: "bucket",
		Key:    "key",
		Body:   bytes.NewReader(make([]byte, MPUThreshold+1)),
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, make([]byte, MPUThreshold+1), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)

	attrs, err = Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))
}

func TestUploadObjectGreaterThanThresholdIfAbsent(t *testing.T) {
	client := objcli.NewTestClient(t, objval.ProviderAWS)

	options := UploadOptions{
		Client:       client,
		Bucket:       "bucket",
		Key:          "key",
		Body:         bytes.NewReader(make([]byte, MPUThreshold+1)),
		Precondition: objcli.OperationPreconditionOnlyIfAbsent,
	}

	attrs, err := Upload(options)
	require.NoError(t, err)

	require.Equal(t, "key", attrs.Key)

	require.NotEmpty(t, attrs.ETag)
	require.True(t, time.Now().After(*attrs.LastModified))

	require.Len(t, client.Buckets, 1)
	require.Len(t, client.Buckets["bucket"], 2)
	require.Contains(t, client.Buckets["bucket"], objval.TestObjectIdentifier{Key: "key"})
	require.Equal(t, make([]byte, MPUThreshold+1), client.Buckets["bucket"][objval.TestObjectIdentifier{Key: "key"}].Body)

	_, err = Upload(options)
	require.Error(t, err)
}
