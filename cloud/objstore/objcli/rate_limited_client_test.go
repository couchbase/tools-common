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

	"github.com/couchbase/tools-common/cloud/objstore/objval"
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

func TestRateLimitedClient_AppendToObject(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	dataToAppend := []byte(strings.Repeat("b", dataSize))
	expData := make([]byte, dataSize, 2*dataSize)
	copy(expData, testData)
	expData = append(expData, dataToAppend...)

	// First, insert an object
	err := rlClient.PutObject(context.Background(), bucket, key, bytes.NewReader(testData))
	require.NoError(t, err)

	// Append the object with new data, and check it takes at least expTimeToGet.
	start := time.Now()
	err = rlClient.AppendToObject(context.Background(), bucket, key, bytes.NewReader(dataToAppend))
	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet))) //nolint:wsl
	require.NoError(t, err)

	// Sanity check that the object is there and contains the correct data.
	obj, err := rlClient.GetObject(context.Background(), bucket, key, nil)
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, expData, data)
}

func TestRateLimitedClient_GetObject(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	// First, insert an object
	err := rlClient.PutObject(context.Background(), bucket, key, bytes.NewReader(testData))
	require.NoError(t, err)

	// Then attempt to retrieve it, and check it takes at least expTimeToGet.
	start := time.Now()
	obj, err := rlClient.GetObject(context.Background(), bucket, key, nil)

	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet)))
	require.NoError(t, err)
	// verify that the object data is correct.
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, testData, data)
}

func TestRateLimitedClient_PutObject(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	// Insert an object, and check it takes at least expTimeToGet to do so.
	start := time.Now()
	err := rlClient.PutObject(context.Background(), bucket, key, bytes.NewReader(testData))

	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet)))
	require.NoError(t, err)

	// Sanity check that the object is there and contains the correct data.
	obj, err := rlClient.GetObject(context.Background(), bucket, key, nil)
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, testData, data)
}

func TestRateLimitedClient_UploadPart(t *testing.T) {
	rlClient := NewRateLimitedClient(NewTestClient(t, objval.ProviderAWS), rate.NewLimiter(1, bytesPerSecond))

	id, err := rlClient.CreateMultipartUpload(context.Background(), bucket, key)
	require.NoError(t, err)

	start := time.Now()
	part, err := rlClient.UploadPart(context.Background(), bucket, id, key, 1, bytes.NewReader(testData))

	require.Greater(t, time.Now(), start.Add(time.Duration(expTimeToGet)))
	require.NoError(t, err)

	err = rlClient.CompleteMultipartUpload(context.Background(), bucket, id, key, part)
	require.NoError(t, err)
}
