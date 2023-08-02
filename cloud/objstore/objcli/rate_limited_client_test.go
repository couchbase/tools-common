package objcli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/couchbase/tools-common/cloud/v2/objstore/objval"
)

const (
	dataSize       = 5
	bytesPerSecond = 1
	// Since the first "burst" isn't rate limited, we subtract 1 to get the expected time.
	expTimeToGet = ((dataSize / bytesPerSecond) - 1)

	bucket = "bucket"
	key    = "key"
)

var testData = []byte(strings.Repeat("a", dataSize))

func TestRateLimitedClientAppendToObject(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	dataToAppend := []byte(strings.Repeat("b", dataSize))
	expData := make([]byte, dataSize, 2*dataSize)
	copy(expData, testData)
	expData = append(expData, dataToAppend...)

	// First, insert an object
	err := rlClient.PutObject(context.Background(), PutObjectOptions{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err)

	// Append the object with new data, and check it takes at least expTimeToGet.
	start := time.Now()

	err = rlClient.AppendToObject(context.Background(), AppendToObjectOptions{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(dataToAppend),
	})
	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet))) //nolint:wsl
	require.NoError(t, err)

	// Sanity check that the object is there and contains the correct data.
	obj, err := rlClient.GetObject(context.Background(), GetObjectOptions{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)

	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, expData, data)
}

func TestRateLimitedClientGetObject(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	// First, insert an object
	err := rlClient.PutObject(context.Background(), PutObjectOptions{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err)

	// Then attempt to retrieve it, and check it takes at least expTimeToGet.
	start := time.Now()

	obj, err := rlClient.GetObject(context.Background(), GetObjectOptions{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)
	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet)))

	// verify that the object data is correct.
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, testData, data)
}

func TestRateLimitedClientPutObject(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	// Insert an object, and check it takes at least expTimeToGet to do so.
	start := time.Now()

	err := rlClient.PutObject(context.Background(), PutObjectOptions{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err)
	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet)))

	// Sanity check that the object is there and contains the correct data.
	obj, err := rlClient.GetObject(context.Background(), GetObjectOptions{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)

	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, testData, data)
}

func TestRateLimitedClientUploadPart(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	id, err := rlClient.CreateMultipartUpload(context.Background(), CreateMultipartUploadOptions{
		Bucket: bucket,
		Key:    key,
	})
	require.NoError(t, err)

	start := time.Now()

	part, err := rlClient.UploadPart(context.Background(), UploadPartOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      key,
		Number:   1,
		Body:     bytes.NewReader(testData),
	})
	require.NoError(t, err)
	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet)))

	err = rlClient.CompleteMultipartUpload(context.Background(), CompleteMultipartUploadOptions{
		Bucket:   bucket,
		UploadID: id,
		Key:      key,
		Parts:    []objval.Part{part},
	})
	require.NoError(t, err)
}
